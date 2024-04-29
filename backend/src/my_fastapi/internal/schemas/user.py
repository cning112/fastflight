from pydantic import BaseModel, EmailStr


class User(BaseModel):
    name: str
    age: int
    email: EmailStr


if __name__ == "__main__":
    print(User.schema_json(indent=2))
