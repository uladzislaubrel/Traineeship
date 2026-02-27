import json
import xml.etree.ElementTree as ET
import os


class Exporter:
    """
    Class responsible for exporting query results into
    specified formats( JSON or XML)
    """

    def __init__(self, output_dir: str = "."):
        self.output_dir = output_dir

    def _export_to_xml_string(
        self, data: dict, root_tag: str = "queries_report", row_tag="query_item"
    ) -> str:
        root = ET.Element(root_tag)

        for section_name, records in data.items():
            section_element = ET.SubElement(root, section_name)
            section_element.text = "\n\t"
            section_element.tail = "\n"

            if not isinstance(records, list):
                ET.SubElement(section_element, "result").text = str(records)
                continue

            for record in records:
                item_element = ET.SubElement(section_element, row_tag)
                item_element.tail = "\n\t"

                for key, value in record.items():
                    ET.SubElement(item_element, key).text = str(value)

        rough_string = ET.tostring(root, "utf-8")
        return rough_string.decode("utf-8")

    def export_results(self, data: dict, format_code: str = "json"):
        if format_code == "1":
            output_format = "json"
        elif format_code == "2":
            output_format = "xml"
        else:
            print("Unknown format code. Export will be in JSON")
            output_format = "json"

        output_filename = f"query_results.{output_format}"
        output_path = os.path.join(self.output_dir, output_filename)

        print(f"Exporting results to {output_path}{output_filename}")

        if output_format == "json":
            self._save_to_json(data, output_path)
        elif output_format == "xml":
            self._save_to_xml(data, output_path)

    def _save_to_json(self, data: dict, output_path: str):
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4, default=str)
            print(f"Results successfully saved to {output_path}")
        except Exception as e:
            print(f"Error during json saving: {e}")

    def _save_to_xml(self, data: dict, output_path: str):
        try:
            xml_string = self._export_to_xml_string(data)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(xml_string)
                print(f"Results successfully saved to {output_path}")
        except Exception as e:
            print(f"Error during xml saving: {e}")
