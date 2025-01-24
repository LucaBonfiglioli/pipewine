import time
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from typing import BinaryIO

import numpy as np

from pipewine.workflows.model import Node, Workflow


def _ccw(p: np.ndarray, q: np.ndarray, r: np.ndarray) -> np.ndarray:
    return (q[..., 0] - p[..., 0]) * (r[..., 1] - p[..., 1]) - (
        q[..., 1] - p[..., 1]
    ) * (r[..., 0] - p[..., 0])


def lines_intersect(
    p1: np.ndarray, p2: np.ndarray, q1: np.ndarray, q2: np.ndarray
) -> np.ndarray:
    o1 = _ccw(p1, p2, q1)
    o2 = _ccw(p1, p2, q2)
    o3 = _ccw(q1, q2, p1)
    o4 = _ccw(q1, q2, p2)
    return (o1 * o2 < 0) & (o3 * o4 < 0)


def lines_intersect_matrix(p1: np.ndarray, p2: np.ndarray) -> np.ndarray:
    p1 = p1[:, np.newaxis, :]
    p2 = p2[:, np.newaxis, :]
    q1 = p1[np.newaxis, :, :]
    q2 = p2[np.newaxis, :, :]
    return lines_intersect(p1, p2, q1, q2)


def lines_rects_intersect_matrix(
    p1: np.ndarray, p2: np.ndarray, xy: np.ndarray, wh: np.ndarray
) -> np.ndarray:
    p1 = p1[:, np.newaxis, :]
    p2 = p2[:, np.newaxis, :]
    w_, h_ = np.zeros_like(wh), np.zeros_like(wh)
    w_[:, 0] = wh[:, 0]
    h_[:, 1] = wh[:, 1]
    q1 = np.concatenate([xy, xy, xy + wh, xy + wh])
    q2 = np.concatenate([xy + w_, xy + h_] * 2)
    q1 = q1[np.newaxis, :, :]
    q2 = q2[np.newaxis, :, :]
    return lines_intersect(p1, p2, q1, q2)


@dataclass
class ViewNode:
    title: str
    fontsize: int
    position: tuple[float, float]
    size: tuple[float, float]
    color: tuple[int, int, int]
    inputs: list[tuple[str, float]]
    outputs: list[tuple[str, float]]


@dataclass
class ViewEdge:
    start: tuple[int, int]
    end: tuple[int, int]


@dataclass
class ViewGraph:
    nodes: list[ViewNode]
    edges: list[ViewEdge]


class Layout(ABC):
    @abstractmethod
    def layout(self, wf: Workflow) -> ViewGraph: ...


class Drawer(ABC):
    @abstractmethod
    def draw(self, vg: ViewGraph, buffer: BinaryIO) -> None: ...


class OptimizedLayout(Layout):
    RGB_SOURCE = (10, 160, 40)
    RGB_OP = (0, 80, 210)
    RGB_SINK = (220, 30, 10)

    def __init__(
        self,
        optimize_steps: int = 200,
        optimize_population: int = 100,
        optimize_time_budget: float = 100.0,
        optimize_noise_start: float = 10.0,
    ) -> None:
        super().__init__()
        self._optimize_steps = optimize_steps
        self._optimize_population = optimize_population
        self._optimize_time_budget = optimize_time_budget
        self._optimize_noise_start = optimize_noise_start

    def _asap_sort(self, workflow: Workflow) -> list[list[Node]]:
        result: list[list[Node]] = []
        queue: deque[Node] = deque()
        deps: dict[Node, set[Node]] = {}
        for node in workflow.get_nodes():
            inbounds = workflow.get_inbound_edges(node)
            deps[node] = {x.src.node for x in inbounds}
            if not inbounds:
                queue.append(node)
        while queue:
            layer: list[Node] = []
            size = len(queue)
            for _ in range(size):
                node = queue.popleft()
                layer.append(node)
                for edge in workflow.get_outbound_edges(node):
                    deps[edge.dst.node].remove(node)
                    if not deps[edge.dst.node]:
                        queue.append(edge.dst.node)
            result.append(layer)
        return result

    def _socket_name(self, socket: int | str | None, default: str) -> str:
        return default if socket is None else str(socket)

    def _optimize(self, vg: ViewGraph) -> None:
        edges = vg.edges
        nodes = vg.nodes

        layout = np.array([x.position for x in nodes], dtype=np.float32)
        sizes = np.array([x.size for x in nodes], dtype=np.float32)
        w, h = layout.max(axis=0) * 1.5
        maxdist = (h**2 + w**2) ** 0.5
        maxsize = sizes.max()
        eps = 1e-3

        def fitness_fn(layout: np.ndarray) -> float:
            edge_start = np.zeros((len(edges), 2), dtype=np.float32)
            edge_end = np.zeros((len(edges), 2), dtype=np.float32)
            for i, x in enumerate(edges):
                sx, sy = layout[x.start[0]]
                ex, ey = layout[x.end[0]]
                sw = sizes[x.start[0]][0]
                edge_start[i] = (sx + sw, sy + nodes[x.start[0]].outputs[x.start[1]][1])
                edge_end[i] = (ex, ey + nodes[x.end[0]].inputs[x.end[1]][1])

            edge_start[:, 0] += eps
            edge_end[:, 0] -= eps

            # Minimize edge length
            dist = np.linalg.norm(edge_end - edge_start, ord=2, axis=-1)
            bound = maxdist - maxsize
            norm_dist = (bound - np.clip(dist - maxsize, 0.0, None)) / bound
            edge_distance_term = (norm_dist**2).mean()

            # Keep nodes reasonably distanced
            # The chebyshev distance with the closest node should be close to the
            # maximum node size.
            cdist = np.max(np.abs(layout[None] - layout[:, None]), axis=-1)
            cdist += np.eye(len(nodes)) * maxdist
            mindist = cdist.min(axis=-1)
            node_distance_term = (np.clip(mindist, None, maxsize) / maxsize).mean()

            # Penalty on backward edges (flow left to right)
            n_backward = (edge_end[:, 0] - edge_start[:, 0] <= maxsize / 2).sum()
            backward_term = (len(edges) - n_backward) / len(edges)

            # Penalty on edge/edge crossings
            mat = lines_intersect_matrix(edge_start, edge_end)
            edge_edge_cross_term = (len(edges) - mat.any(-1).sum()) / len(edges)

            # Penalty on edge/node crossings
            mat = lines_rects_intersect_matrix(edge_start, edge_end, layout, sizes)
            edge_node_cross_term = (len(edges) - mat.any(-1).sum()) / len(edges)

            return (
                node_distance_term
                + edge_distance_term
                + backward_term
                + edge_edge_cross_term
                + edge_node_cross_term
            ) / 5

        def spawn(layout: np.ndarray, sigma: float) -> np.ndarray:
            noise = np.random.normal(0.0, sigma, layout.shape).astype(np.float32)
            return layout + noise

        global_step = 0
        max_steps = self._optimize_steps
        max_population = self._optimize_population
        sigma_s = self._optimize_noise_start
        sigma_e = sigma_s * 1e-4
        layouts = [spawn(layout, sigma_s) for _ in range(max_population - 1)] + [layout]
        best = -float("inf")
        argbest = layout
        fitness = np.zeros((max_population,), dtype=np.float32)
        t_start = time.time()
        while True:
            if (
                time.time() - t_start > self._optimize_time_budget
                or global_step > max_steps
            ):
                break
            sigma = sigma_s * np.exp(
                np.log(sigma_e / sigma_s) * global_step / max_steps
            )
            for i, layout in enumerate(layouts):
                fitness[i] = fitness_fn(layout)
                if fitness[i] > best:
                    best = fitness[i]
                    argbest = layout
            fitness = (fitness - fitness.min()) / (fitness.max() - fitness.min() + 1e-5)
            fsum = fitness.sum()
            if fsum > 0:
                next_idx = np.random.choice(
                    max_population, size=max_population, p=fitness / fsum
                )
            else:
                next_idx = np.arange(max_population)
            for i, idx in enumerate(next_idx):
                layouts[i] = spawn(layouts[idx], sigma)
            global_step += 1

        minxy = argbest.min(axis=0)
        for i, xy in enumerate(argbest):
            vg.nodes[i].position = tuple(xy - minxy)

    def layout(self, wf: Workflow) -> ViewGraph:
        view_nodes: dict[Node, ViewNode] = {}
        view_edges: list[ViewEdge] = []
        node_to_idx: dict[Node, int] = {}
        i = 0
        margin = 32
        node_distance = 96
        fontsize = 16
        current_x = 0.0
        for layer in self._asap_sort(wf):
            current_y = 0.0
            maxw = 0.0
            for node in layer:
                node_to_idx[node] = i
                inputs: set[str] = set()
                outputs: set[str] = set()
                for e in wf.get_inbound_edges(node):
                    inputs.add(self._socket_name(e.dst.socket, "input"))
                for e in wf.get_outbound_edges(node):
                    outputs.add(self._socket_name(e.src.socket, "output"))
                if len(inputs) == 0:
                    col = self.RGB_SOURCE
                elif len(outputs) == 0:
                    col = self.RGB_SINK
                else:
                    col = self.RGB_OP

                title_w, title_h = get_text_size(node.name, fontsize)
                in_sockets_w = in_sockets_h = out_sockets_w = out_sockets_h = 0.0
                in_sockets: list[tuple[str, float]] = []
                out_sockets: list[tuple[str, float]] = []
                current = title_h
                for socket in sorted(inputs):
                    sw, sh = get_text_size(socket, fontsize)
                    in_sockets_w = max(in_sockets_w, sw)
                    in_sockets_h += margin + sh
                    current += margin + sh
                    in_sockets.append((socket, current))
                current = title_h
                for socket in sorted(outputs):
                    sw, sh = get_text_size(socket, fontsize)
                    out_sockets_w = max(out_sockets_w, sw)
                    out_sockets_h += margin + sh
                    current += margin + sh
                    out_sockets.append((socket, current))

                w = max(title_w, in_sockets_w + out_sockets_w) + margin
                h = title_h + max(in_sockets_h, out_sockets_h) + margin

                view_nodes[node] = ViewNode(
                    title=node.name,
                    fontsize=fontsize,
                    position=(float(current_x), float(current_y)),
                    size=(w, h),
                    color=col,
                    inputs=in_sockets,
                    outputs=out_sockets,
                )
                current_y += h + node_distance
                maxw = max(maxw, w)
                i += 1
            current_x += maxw + node_distance
        for node in wf.get_nodes():
            for e in wf.get_outbound_edges(node):
                src_node = view_nodes[node]
                dst_node = view_nodes[e.dst.node]
                src_node_idx = node_to_idx[node]
                dst_node_idx = node_to_idx[e.dst.node]
                src_s_idx = [x[0] for x in src_node.outputs].index(
                    self._socket_name(e.src.socket, "output")
                )
                dst_s_idx = [x[0] for x in dst_node.inputs].index(
                    self._socket_name(e.dst.socket, "input")
                )
                view_edges.append(
                    ViewEdge((src_node_idx, src_s_idx), (dst_node_idx, dst_s_idx))
                )
        vg = ViewGraph(list(view_nodes.values()), view_edges)
        if len(view_nodes) > 1:
            self._optimize(vg)
        return vg


def get_text_size(text: str, font_size: int) -> tuple[float, float]:
    text_height = font_size
    average_char_width = 0.6 * font_size
    text_width = len(text) * average_char_width
    return text_width, text_height


class SVGDrawer(Drawer):
    RGB_TEXT = "white"

    def _draw_node(self, parent: ET.Element, node: ViewNode) -> None:
        text = node.title
        w, h = node.size
        x, y = node.position
        fontsize = node.fontsize

        col = f"rgb{node.color}"
        ET.SubElement(
            parent,
            "rect",
            x=str(x),
            y=str(y),
            width=str(w),
            height=str(h),
            fill=col,
            rx="10",
        )
        text_elem = ET.SubElement(
            parent,
            "text",
            x=str(x + w / 2),
            y=str(y + (fontsize + 10) / 2),
            fill="white",
        )
        text_elem.set("text-anchor", "middle")
        text_elem.set("dominant-baseline", "central")
        text_elem.set("font-size", str(fontsize))
        text_elem.set("font-family", "Arial")
        text_elem.set("font-weight", "bold")
        text_elem.text = text

        for socket, sy in node.inputs:
            circle = ET.SubElement(
                parent,
                "circle",
                cx=str(x),
                cy=str(y + sy),
                r="8",
                fill="white",
                stroke=col,
            )
            circle.set("stroke-width", "2")
            text_elem = ET.SubElement(
                parent, "text", x=str(x + 12), y=str(y + sy), fill="white"
            )
            text_elem.set("dominant-baseline", "central")
            text_elem.set("font-size", str(fontsize - 4))
            text_elem.set("font-family", "Arial")
            text_elem.text = socket
        for socket, sy in node.outputs:
            circle = ET.SubElement(
                parent,
                "circle",
                cx=str(x + w),
                cy=str(y + sy),
                r="8",
                fill="white",
                stroke=col,
            )
            circle.set("stroke-width", "2")
            text_elem = ET.SubElement(
                parent, "text", x=str(x + w - 12), y=str(y + sy), fill="white"
            )
            text_elem.set("text-anchor", "end")
            text_elem.set("dominant-baseline", "central")
            text_elem.set("font-size", str(fontsize - 4))
            text_elem.set("font-family", "Arial")
            text_elem.text = socket

    def _draw_edge(
        self, parent: ET.Element, x1: float, y1: float, x2: float, y2: float
    ) -> None:
        x1 += 9
        x2 -= 9
        head_size = 15
        xk = x2 - head_size / 2
        tn = 0.5 * np.pi * (y1 - y2) / (x1 - x2) * np.sin(np.pi * (xk - x1) / (x2 - x1))
        angle = np.arctan(tn) if x1 < x2 else np.pi - np.arctan(tn)
        h1 = (x2 - x1) * (0.5 - 0.134)
        h2 = (x2 - x1) * (0.5 + 0.134)
        line = ET.SubElement(
            parent,
            "path",
            d=f"M {x1} {y1} C {x1 + h1} {y1} {x1 + h2} {y2} {x2} {y2}",
            stroke="black",
            fill="none",
        )
        line.set("stroke-width", "2")
        line.set("stroke-linecap", "round")
        tip = ET.SubElement(
            parent,
            "path",
            d=f"M0,0 L-{head_size - 4},-{head_size / 2 - 2} V{head_size / 2 - 2} Z",
            fill="black",
            stroke="black",
            transform=f"translate({x2}, {y2}) rotate({angle * 180 / np.pi})",
        )
        tip.set("stroke-width", "4")
        tip.set("stroke-linejoin", "round")

    def draw(self, vg: ViewGraph, buffer: BinaryIO) -> None:
        svg = ET.Element("svg", xmlns="http://www.w3.org/2000/svg")
        nodes_group = ET.Element("g")
        edges_group = ET.Element("g")
        groups: list[ET.Element] = []
        for node in vg.nodes:
            group = ET.SubElement(nodes_group, "g")
            self._draw_node(group, node)
            groups.append(group)
        for edge in vg.edges:
            src_node_idx, src_sock = edge.start
            dst_node_idx, dst_sock = edge.end
            src_node, dst_node = vg.nodes[src_node_idx], vg.nodes[dst_node_idx]
            x1 = src_node.position[0] + src_node.size[0]
            y1 = src_node.position[1] + src_node.outputs[src_sock][1]
            x2 = dst_node.position[0]
            y2 = dst_node.position[1] + dst_node.inputs[dst_sock][1]
            self._draw_edge(edges_group, x1, y1, x2, y2)

        svg.append(nodes_group)
        svg.append(edges_group)
        tree = ET.ElementTree(svg)
        svg.set("width", str(max(node.position[0] + node.size[0] for node in vg.nodes)))
        svg.set(
            "height", str(max(node.position[1] + node.size[1] for node in vg.nodes))
        )
        tree.write(buffer)
