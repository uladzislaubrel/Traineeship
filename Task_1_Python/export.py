import json
import xml.etree.ElementTree as ET

def export_to_xml(data, root_tag="analyze_report", row_tag="query_item"):

    root = ET.Element(root_tag)

    for section_name, records in data.items():
        section_element = ET.SubElement(root, section_name)
        section_element.text = '\n\t'
        section_element.tail = '\n'


        for record in records:
            item_element = ET.SubElement(section_element, row_tag)
            item_element.tail = '\n\t'

            for key, value in record.items():
                ET.SubElement(item_element, key).text = str(value)


    rough_string = ET.tostring(root, encoding='utf-8')

    return rough_string.decode('utf-8')

def export_results(data, format_code):  #, query):

    if format_code == '1':
        output_format = 'json'

    elif format_code == '2':
        output_format = 'xml'

    else:
        print("Неизвестный код формата. Экспорт будет в формате JSON.")
        output_format = 'json'

    output_path = f"query_results.{output_format}"

    if output_format == 'json':
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4, default=str)
            print(f"Результаты успешно сохранены в файл: {output_path}")
        except Exception as e:
            print(f"Ошибка при сохранении в JSON: {e}")

    elif output_format == 'xml':
        try:
            xml_string = export_to_xml(data)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(xml_string)
            print(f"Результаты успешно сохранены в файл: {output_path}")

        except Exception as e:
            print(f"Ошибка при сохранении в XML: {e}")