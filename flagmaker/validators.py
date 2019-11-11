from prompt_toolkit.validation import ValidationError
from prompt_toolkit.validation import Validator
from typing import List

class ChoiceValidator(Validator):
    def __init__(self, choices):
        self.choices: List[str] = choices

    def validate(self, document):
        text = document.text
        if text not in map(lambda x: x[0:len(text)], self.choices):
            raise ValidationError(
                message='Invalid choice. Options: {}'
                    .format(', '.join(self.choices))
            )
