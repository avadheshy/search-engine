class CommonUtils:
    @classmethod
    def get_limit_offset_with_page_no_and_page_limit(cls, page, page_limit):
        offset = 0
        limit = 15
        if page and page_limit:
            offset = (page - 1) * page_limit
            limit = page_limit
        return limit, offset
