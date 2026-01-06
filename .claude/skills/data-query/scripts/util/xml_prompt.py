import json

from app.context.neo4j_schema import Column, Knowledge, TableInfo


def _tag(tag: str, val, is_json: bool = False):
    if (
        (not isinstance(val, (bool, int, float)))
        and (not val)
        and (isinstance(val, str) and not val.strip())
    ):
        return ""
    content = json.dumps(val, ensure_ascii=False) if is_json else val
    return f"<{tag}>{content}</{tag}>"


def tb_info_xml_str(
    tb_map: dict[str, TableInfo], col_map: dict[str, dict[str, Column]]
):
    """
    构建XML格式的表字段信息字符串

    <tables>
        <table>
            <table_code></table_code>
            <table_name></table_name>
            <table_meaning></table_meaning>
            <columns>
                <column><column_name></column_name><column_comment></column_comment>...</column>
                <column><column_name></column_name><column_comment></column_comment>...</column>
            </columns>
        </table>
    </tables>
    """

    def build_col_xml_str(col_dict: dict[str, Column]):
        """构建XML格式的字段信息字符串"""
        return "".join(
            [
                """
            <column>"""
                f"{_tag('column_name', c.col_name)}"
                f"{_tag('column_comment', c.col_comment)}"
                f"{_tag('column_meaning', c.col_meaning)}"
                f"{_tag('column_alias', c.col_alias)}"
                f"{_tag('column_json_meaning', c.field_meaning, True)}"
                f"{_tag('fewshot', c.fewshot)}"
                f"{_tag('cells', c.cells)}"
                "</column>"
                for c in col_dict.values()
            ]
        )

    tb_xml_str = "".join(
        [
            f"""
    <table>
        <table_code>{tb_code}</table_code>
        <table_name>{tb_map[tb_code].tb_name}</table_name>
        <table_meaning>{tb_map[tb_code].tb_meaning}</table_meaning>
        <columns>{build_col_xml_str(col_dict)}
        </columns>
    </table>"""
            for tb_code, col_dict in col_map.items()
        ]
    )
    tb_xml_str = f"<tables>{tb_xml_str}\n</tables>"
    return tb_xml_str


def kn_info_xml_str(kn_map: dict[int, Knowledge]):
    """构建XML格式的指标知识信息字符串"""

    kn_xml_str = "".join(
        [
            f"""
    <knowledge>"""
            f"{_tag('kn_code', k.kn_code)}"
            f"{_tag('kn_name', k.kn_name)}"
            f"{_tag('kn_def', k.kn_def)}"
            f"{_tag('kn_desc', k.kn_desc)}"
            f"{_tag('rel_kn', k.rel_kn)}"
            f"{_tag('kn_alias', k.kn_alias)}"
            "</knowledge>"
            for k in kn_map.values()
        ]
    )
    kn_xml_str = f"<knowledges>{kn_xml_str}\n</knowledges>"
    return kn_xml_str


def sql_result_xml_str(result_list: list[list[dict[str, str]]]):
    """
    构建SQL语句与查询结果字符串

    <sql_results>
        <id></id>
        <sql_result>
            <query></query>
            <sql></sql>
            <result></result>

            <query></query>
            <sql></sql>
            <error></error>
        </sql_result>
    </sql_results>
    """

    xml_str_list: list[str] = []
    xml_str_list.append("<sql_results>")

    for idx, result in enumerate(result_list):
        xml_str_list.append(f"\t<id>{idx}</id>")
        xml_str_list.append("\t<sql_result>")
        xml_str_list.append(
            "\n\n".join(
                [
                    f"\t\t<query>{r['query']}</query>\n"
                    f"\t\t<sql>{r['sql']}</sql>\n"
                    f"\t\t{_tag('result' if 'result' in r else 'error', r.get('result') or r.get('error'))}"
                    for r in result
                ]
            )
        )

        xml_str_list.append("\t</sql_result>")

    xml_str_list.append("</sql_results>")

    return "\n".join(xml_str_list)
