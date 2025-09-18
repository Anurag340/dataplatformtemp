class TagUtils:
    @staticmethod
    def extract_goal(strings):
        """
        Extracts the entry from a list of strings where the delimiter is 'Goal:<some-string>'.
        """
        if strings is not None:
            for string in strings:
                if string.startswith("Goal:"):
                    return string.replace("Goal:", "", 1).strip()
        return None

    @staticmethod
    def extract_stage(strings):
        """
        Extracts the entry from a list of strings where the delimiter is 'Goal:<some-string>'.
        """
        if strings is not None:
            for string in strings:
                if string.startswith("Stage:"):
                    return string.replace("Stage:", "", 1).strip()
        return None
