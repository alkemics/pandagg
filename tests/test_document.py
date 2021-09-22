import pytest

from pandagg import Mappings
from pandagg.document import InnerDocSource, DocumentSource
from pandagg.mappings import Text, Long, Date, Keyword, Object, Nested
from pandagg.node.mappings import Boolean


class User(InnerDocSource):

    id: int = Long(required=True)
    signed_up: str = Date()
    username: str = Text(fields={"keyword": Keyword()}, required=True, multiple=False)
    email = Text(fields={"keyword": Keyword()})
    location = Text(fields={"keyword": Keyword()})


class Comment(InnerDocSource):

    author = Object(properties=User, required=True)
    created = Date(required=True)
    content = Text(required=True)


class Post(DocumentSource):

    author = Object(properties=User, required=True)
    created = Date(required=True)
    body = Text(required=True)
    comments = Nested(properties=Comment, multiple=True)


def test_document_init():
    user = User(id=1, signed_up="2021-01-01")
    assert user.id == 1
    assert user.signed_up == "2021-01-01"
    assert isinstance(user._mappings_, Mappings)
    assert user._mappings_.to_dict() == {
        "properties": {
            "email": {"fields": {"keyword": {"type": "keyword"}}, "type": "text"},
            "id": {"type": "long"},
            "location": {"fields": {"keyword": {"type": "keyword"}}, "type": "text"},
            "signed_up": {"type": "date"},
            "username": {"fields": {"keyword": {"type": "keyword"}}, "type": "text"},
        }
    }
    assert list(user._field_attrs_.keys()) == [
        "id",
        "signed_up",
        "username",
        "email",
        "location",
    ]
    with pytest.raises(TypeError) as e:
        User(fake_field=1)
    assert e.value.args == (
        "'fake_field' is an invalid keyword argument for <class 'tests.test_document.User'>",
    )


def test_pre_save(write_client):
    class AutoDatePost(DocumentSource):

        author = Object(properties=User, required=True)
        updated = Date(required=True)
        body = Text(required=True)
        comments = Nested(properties=Comment, multiple=True)

        def _pre_save_op_(self):
            if self.updated is None:
                # TODO - handle datetime serialization
                self.updated = "2021-01-01"

    post = AutoDatePost(author=User(id=1, username="paul"), body="knock knock")
    post._pre_save_op_()
    assert post.updated == "2021-01-01"


def test_post_init(write_client):
    class AutoArchivedPost(DocumentSource):

        updated = Date(required=True)
        archived = Boolean()

        def _post_init_(self) -> None:
            # automatically archive post that hasn't been updated in a while
            if self.updated and self.updated < "2020":
                self.archived = True

    post = AutoArchivedPost(archived=False)
    assert post.archived is False

    post = AutoArchivedPost(updated="1920", archived=False)
    assert post.archived is True


def test_nested_document_to_dict():
    paul = User(id=1, username="paul")
    chani = User(id=2, username="chani")

    post = Post(
        author=paul,
        body="knock knock",
        comments=[
            Comment(author=chani, content="who's there?"),
            Comment(author=paul, content="it's me"),
        ],
    )
    assert post._to_dict_() == {
        "author": {"id": 1, "username": "paul"},
        "body": "knock knock",
        "comments": [
            {"author": {"id": 2, "username": "chani"}, "content": "who's there?"},
            {"author": {"id": 1, "username": "paul"}, "content": "it's me"},
        ],
    }


def test_doc_deserialization():
    post = Post._from_dict_(
        {
            "author": {"id": 1, "username": "paul"},
            "body": "knock knock",
            "comments": [
                {"author": {"id": 2, "username": "chani"}, "content": "who's there?"},
                {"author": {"id": 1, "username": "paul"}, "content": "it's me"},
            ],
        }
    )
    assert isinstance(post.author, User)
    assert post.author.id == 1
    assert post.author.username == "paul"
    assert post.body == "knock knock"
    assert len(post.comments) == 2
    assert post.comments[0].author.id == 2
    assert post.comments[0].author.username == "chani"
    assert post.comments[0].content == "who's there?"
    assert post.comments[1].author.id == 1
    assert post.comments[1].author.username == "paul"
    assert post.comments[1].content == "it's me"

    # strict fails
    with pytest.raises(TypeError) as e:
        Post._from_dict_(
            {
                "author": {"id": 1, "username": "paul"},
                "body": "knock knock",
                "comments": [
                    {
                        "author": {"id": 2, "username": "chani"},
                        "content": "who's there?",
                    },
                    {
                        "author": {"id": 1, "username": ["paul", "paulo"]},
                        "content": "it's me",
                    },
                ],
            }
        )
    assert e.value.args == (
        "Unexpected list for field comments.author.username, got ['paul', 'paulo']",
    )

    # not strict is ok
    post = Post._from_dict_(
        {
            "author": {"id": 1, "username": "paul"},
            "body": "knock knock",
            "comments": [
                {"author": {"id": 2, "username": "chani"}, "content": "who's there?"},
                {
                    "author": {"id": 1, "username": ["paul", "paulo"]},
                    "content": "it's me",
                },
            ],
        },
        strict=False,
    )
    assert post.comments[1].author.username == ["paul", "paulo"]


def test_nested_document_to_dict_empty_multiple():
    # with empty 'multiple' field: -> [] instead of None
    post = Post(author=User(id=1, username="paul"), body="knock knock")
    assert post.comments == []
    assert post._to_dict_(with_empty_keys=True) == {
        "author": {
            "email": None,
            "id": 1,
            "location": None,
            "signed_up": None,
            "username": "paul",
        },
        "body": "knock knock",
        "comments": [],
        "created": None,
    }


def test_document_to_dict():
    user = User(id=1, signed_up="2021-01-01")
    assert user._to_dict_() == {"id": 1, "signed_up": "2021-01-01"}
    assert user._to_dict_(with_empty_keys=True) == {
        "email": None,
        "id": 1,
        "location": None,
        "signed_up": "2021-01-01",
        "username": None,
    }
