import pytest

from typelime._register import RegisterMeta


@pytest.fixture
def register_hierarchy():
    RegisterMeta.reset()

    class Root(metaclass=RegisterMeta, title="root", namespace="test"):
        pass

    class A(Root, metaclass=RegisterMeta, title="a", namespace="test"):
        pass

    class B(Root, metaclass=RegisterMeta, title="b", namespace="test"):
        pass

    class C(B, metaclass=RegisterMeta, title="c", namespace="test"):
        pass

    class D(B, metaclass=RegisterMeta, title="d", namespace="test"):
        pass

    return Root


class TestRegisterMeta:
    @pytest.mark.parametrize(
        "title", ["", "test", "title-dash", "title_special!@${}()[]"]
    )
    def test_title(self, title: str):
        class A(metaclass=RegisterMeta, title=title):
            pass

        assert A.title == title

    def test_title_default(self):
        class A(metaclass=RegisterMeta):
            pass

        assert A.title == "A"

    @pytest.mark.parametrize("ns", ["", "test", "ns-dash", "ns_special!@${}()[]"])
    def test_namespace(self, ns: str):
        class A(metaclass=RegisterMeta, namespace=ns):
            pass

        assert A.namespace == ns

    def test_namespace_default(self):
        class A(metaclass=RegisterMeta):
            pass

        assert isinstance(A.namespace, str)

    def test_registered(self, register_hierarchy):
        root = register_hierarchy
        assert len(root.registered) == 5

    def test_get(self, register_hierarchy):
        root = register_hierarchy
        assert root.get("test.a").title == "a"
        assert root.get("a") is None
