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


def do_intersect(
    p1: np.ndarray, p2: np.ndarray, q1: np.ndarray, q2: np.ndarray
) -> np.ndarray:
    o1 = _ccw(p1, p2, q1)
    o2 = _ccw(p1, p2, q2)
    o3 = _ccw(q1, q2, p1)
    o4 = _ccw(q1, q2, p2)
    return (o1 * o2 < 0) & (o3 * o4 < 0)


def cross_inter(segments_start: np.ndarray, segments_end: np.ndarray) -> np.ndarray:
    p1 = segments_start[:, np.newaxis, :]
    p2 = segments_end[:, np.newaxis, :]
    q1 = segments_start[np.newaxis, :, :]
    q2 = segments_end[np.newaxis, :, :]
    return do_intersect(p1, p2, q1, q2)


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


class AsapLayout(Layout):
    RGB_SOURCE = (10, 160, 40)
    RGB_OP = (0, 80, 210)
    RGB_SINK = (220, 30, 10)

    def __init__(
        self,
        optimize_steps: int = 1000,
        optimize_population: int = 100,
        optimize_time_budget: float = 5.0,
    ) -> None:
        super().__init__()
        self._optimize_steps = optimize_steps
        self._optimize_population = optimize_population
        self._optimize_time_budget = optimize_time_budget

    def _asap_sort(self, workflow: Workflow) -> list[list[Node]]:
        result: list[list[Node]] = []
        queue: deque[Node] = deque()
        mark: set[Node] = set()
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
                if node in mark:
                    continue
                layer.append(node)
                mark.add(node)
                for edge in workflow.get_outbound_edges(node):
                    deps[edge.dst.node].remove(node)
                    if not deps[edge.dst.node]:
                        queue.append(edge.dst.node)
            if layer:
                result.append(layer)
        return result

    def _socket_name(self, socket: int | str | None, default: str) -> str:
        return default if socket is None else str(socket)

    def _optimize(self, vg: ViewGraph) -> None:
        edges = vg.edges
        nodes = vg.nodes

        layout = np.array([x.position for x in nodes], dtype=np.float32)
        w, h = layout.max(axis=0) * 1.5
        maxdist = (h**2 + w**2) ** 0.5
        maxsize = np.array([x.size for x in nodes], dtype=np.float32).max()

        def fitness_fn(layout: np.ndarray) -> float:
            edge_start = np.zeros((len(edges), 2), dtype=np.float32)
            edge_end = np.zeros((len(edges), 2), dtype=np.float32)
            for i, x in enumerate(edges):
                sx, sy = layout[x.start[0]]
                ex, ey = layout[x.end[0]]
                sw = nodes[x.start[0]].size[0]
                edge_start[i] = (sx + sw, sy + nodes[x.start[0]].outputs[x.start[1]][1])
                edge_end[i] = (ex, ey + nodes[x.end[0]].inputs[x.end[1]][1])

            dist = np.linalg.norm(edge_end - edge_start, ord=2, axis=-1)
            edge_distance_term = np.clip(
                (maxdist - dist) / (maxdist - maxsize), 0.0, 1.0
            ).mean()

            cdist = np.max(np.abs(layout[None] - layout[:, None]), axis=-1)
            cdist += np.eye(len(nodes)) * maxdist
            mindist = cdist.min(axis=-1)
            node_distance_term = (np.clip(mindist, None, maxsize) / (maxsize)).mean()

            n_backward = (edge_end[:, 0] - edge_start[:, 0] <= maxsize / 2).sum()
            backward_term = (len(edges) - n_backward) / len(edges)

            n_cross = cross_inter(edge_start, edge_end).sum()
            maxcross = len(edges) ** 2
            cross_term = (maxcross - n_cross) / maxcross

            return (
                node_distance_term + edge_distance_term + cross_term + backward_term
            ) / 4

        def spawn(layout: np.ndarray) -> np.ndarray:
            noise = np.random.normal(0.0, 10, layout.shape).astype(np.float32)
            result: np.ndarray = layout + noise
            return result

        N = self._optimize_steps
        P = self._optimize_population
        layouts = [spawn(layout) for _ in range(P - 1)] + [layout]
        best = -float("inf")
        argbest = layout
        fitness = np.zeros((P,), dtype=np.float32)
        t_start = time.time()
        for _ in range(N):
            if time.time() - t_start > self._optimize_time_budget or best >= 1.0:
                break
            for i, layout in enumerate(layouts):
                fitness[i] = fitness_fn(layout)
                if fitness[i] > best:
                    best = fitness[i]
                    argbest = layout
            fitness = (fitness - fitness.min()) / (fitness.max() - fitness.min() + 1e-5)
            fsum = fitness.sum()
            if fsum > 0:
                next_idx = np.random.choice(P, size=P, p=fitness / fsum)
            else:
                next_idx = np.arange(P)
            for i, idx in enumerate(next_idx):
                layouts[i] = spawn(layouts[idx])

        minxy = argbest.min(axis=0)
        for i, xy in enumerate(argbest):
            vg.nodes[i].position = tuple(xy - minxy)

    def layout(self, wf: Workflow) -> ViewGraph:
        view_nodes: dict[Node, ViewNode] = {}
        view_edges: list[ViewEdge] = []
        node_to_idx: dict[Node, int] = {}
        i = 0
        margin = 32
        fontsize = 16
        current_x = 0
        for layer in self._asap_sort(wf):
            current_y = 0
            maxw = 0
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
                current_y += h + margin
                maxw = max(maxw, w)
                i += 1
            current_x += maxw + margin
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
        rect_elem = ET.SubElement(
            parent,
            "rect",
            x=str(x),
            y=str(y),
            width=str(w),
            height=str(h),
            fill=col,
            rx="10",
        )
        rect_elem.set("fill-opacity", "0.9")
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
        h = (x2 - x1) / 2
        line = ET.SubElement(
            parent,
            "path",
            d=f"M {x1} {y1} C {x1 + h} {y1} {x1 + h} {y2} {x2} {y2}",
            stroke="black",
            fill="none",
        )
        line.set("stroke-width", "4")

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

        svg.append(edges_group)
        svg.append(nodes_group)
        tree = ET.ElementTree(svg)
        svg.set("width", str(max(node.position[0] + node.size[0] for node in vg.nodes)))
        svg.set(
            "height", str(max(node.position[1] + node.size[1] for node in vg.nodes))
        )
        tree.write(buffer)


if __name__ == "__main__":
    from pathlib import Path

    from pipewine.operators import *
    from pipewine.sinks import *
    from pipewine.sources import *

    wf = Workflow()
    data1 = wf.node(UnderfolderSource(folder=Path("i1")))()
    data2 = wf.node(UnderfolderSource(folder=Path("i2")))()
    repeat = wf.node(RepeatOp(1000))(data1)
    slice_ = wf.node(SliceOp(step=2))(repeat)
    wf.node(UnderfolderSink(Path("o1")))(slice_)
    cat = wf.node(CatOp())([data1, data2, repeat])
    wf.node(UnderfolderSink(Path("o2")))(cat)
    cat2 = wf.node(CatOp())([cat, slice_, data2])
    wf.node(UnderfolderSink(Path("o3")))(cat2)

    drawer = SVGDrawer()
    layout = AsapLayout()

    with open("/tmp/file.svg", "wb") as fp:
        drawer.draw(layout.layout(wf), fp)
