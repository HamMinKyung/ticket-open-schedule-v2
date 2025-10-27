from notion_writer.writer import NotionRepository

def main():
    # NotionRepository 인스턴스 생성
    repo = NotionRepository()
    repo.sync_existing_ticket_relations()

if __name__ == "__main__":
    main()
