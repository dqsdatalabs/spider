# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import json
import logging
import re
import scrapy
from scrapy.http import JsonRequest
from scrapy import Request
from ..loaders import ListingLoader
from ..helper import *
import math


class DemeesterSpider(scrapy.Spider):
    """
    not able to read  cost as it's in description field with bad format
    pets_allowed not work when call item_loader.add_value("pets_allowed", False)
    """

    name = "demeester"
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","
    allowed_domains = ["demeester.be"]

    start_urls = [
        "https://www.demeester.be/te-huur/appartement/",
        "https://www.demeester.be/te-huur/huis/",
    ]

    def parse(self, response, **kwargs):
        """parse list page and send requests to detail page, read fields if exist """
        token = self.sub_string_between(response.text, "media_token =", "';").replace("'", "").strip()
        url = "https://www.demeester.be/ajax.asmx/GetPropertiesList"
        form_data = {
            "oForm": {
                "bedrooms": 0,
                "CheckedTypes": "0",
                "City": "null",
                "CompanyID": 0,
                "CountryExclude": "",
                "CountryInclude": "",
                "currentPage": "0",
                "Environments": "",
                "ExtraSQL": "0",
                "ExtraSQLFilters": "0",
                "FilterOutTypes": "21",
                "Headtypes_Underlyingprops_Sort_Desc": "0",
                "homeSearch": False,
                "investment": False,
                "Language": "nl",
                "latitude": "0",
                "longitude": "0",
                "MaxBedrooms": "0",
                "MaxBuildYear": "0",
                "MaxPrice": "",
                "MaxSurface": "0",
                "MaxSurfaceGround": "0",
                "MediaID": 0,
                "menuIDUmbraco": "0",
                "MinBedrooms": "0",
                "MinBuildYear": "0",
                "MinPrice": "",
                "MinSurface": "0",
                "MinSurfaceGround": "0",
                "NavigationItem": 0,
                "newbuilding": "-1",
                "NumResults": 0,
                "officeID": "0",
                "OrderBy": "0",
                "PageName": "0",
                "PriceClass": "0",
                "PropertyID": "0",
                "PropertyName": "0",
                "Radius": None,
                "Region": "0",
                "ShowChildrenInsteadOfProject": "false",
                "ShowProjects": False,
                "SliderItem": "0",
                "SliderStep": "0",
                "SortField": "1",
                "SQLType": 3,
                "StartIndex": 1,
                "state": "0",
                "Token": token,
                "Transaction": None,
                "Type": None,
                "useCheckBoxes": "0",
                "UseHeadType": True,
                "UsePriceClass": True,
            },
            "sStoreCriteria": "0",
            "sUrl": response.url,
        }
        yield JsonRequest(
            url=url,
            data=form_data,
            dont_filter=True,
            callback=self.after_post,
            headers=self.get_lang(),
            method="POST",
            cb_kwargs=dict(form=form_data),
        )

    def after_post(self, response, form):
        """ parse json """
        results = json.loads(response.text)["d"]["propertyList"]
        if len(results) >= 20:
            page = form["oForm"]["StartIndex"] + 1
            form["oForm"]["StartIndex"] = page
            form_data = form
            yield JsonRequest(
                url=response.url,
                data=form_data,
                dont_filter=True,
                callback=self.after_post,
                headers=self.get_lang(),
                method="POST",
                cb_kwargs=dict(form=form_data),
            )
        for obj in results:
            obj["Property_Type_Value"] = "apartment" if "appartement" in form["sUrl"] else "house"
            yield Request(
                response.urljoin(obj["Property_URL"].split("0/")[0]),
                self.parse_detail,
                dont_filter=True,
                headers={"Referer": response.request.headers["Referer"]},
                cb_kwargs=dict(obj=obj),
            )

    def parse_detail(self, response, obj):
        """parse detail page """
        main_block = response.xpath(".//*[@id='overzicht']")
        if len(main_block) == 1:
            item_loader = ListingLoader(response=response)
            item_loader.add_value(
                "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
            )
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("property_type", obj["Property_Type_Value"])
            # item_loader.add_value("external_link", response.urljoin(obj["Property_URL"].split("0/")[0]))
            rent = obj["Property_Price"]
            if rent:
                price = rent.replace(" ","").replace("\xa0","")
                item_loader.add_value("rent_string", price)
            # item_loader.add_value("currency", currency_parser(obj["Property_Price"]))
            item_loader.add_value("city", obj["Property_City_Value"])
            item_loader.add_value("zipcode", obj["Property_Zip"])
            item_loader.add_value("room_count", obj["bedrooms"])
            item_loader.add_value("latitude", str(obj["Property_Lat"]))
            item_loader.add_value("longitude", str(obj["Property_Lon"]))
            item_loader.add_value("external_id", obj["Property_Reference"])
            item_loader.add_value("title", obj["Property_Title"])
            if obj["Property_Furniture"] != "-1" and obj["Property_Furniture"] != "0":
                item_loader.add_value("furnished", True)
            # self.parse_map(response, item)

            square_meters = response.xpath("//tr/th[contains(.,'Bewoonbare')]/following-sibling::td/text()").get()
            if square_meters:
                square_meters = square_meters.split(" ")[0].replace(",",".")
                item_loader.add_value("square_meters", str(int(float(square_meters))))
            
            utilities = response.xpath("//tr/th[contains(.,'kosten')]/following-sibling::td/text()").get()
            if utilities:
                item_loader.add_value("utilities", utilities)
            
            parking = response.xpath("//li//span[contains(.,'Garage')]/text()").get()
            if parking:
                item_loader.add_value("parking", True)
            
            detail_node = response.xpath(".//*[@class='prop-features']")
            item_loader.add_xpath("address", "//div/h1/parent::div/p[@class='address']//text()")
            item_loader.add_xpath("description", ".//*[@id='overzicht']//div[@class='prop-descr']//text()")

            item_loader.add_xpath("images", ".//div[@class='prop-imgs']//img/@src")

            self.get_from_detail_panel(" ".join(detail_node.xpath(".//li/text()").getall()), item_loader)

            item_loader.add_xpath("landlord_name", ".//*[contains(@class,'contactinfo')]//h3//text()")

            item_loader.add_xpath("landlord_phone", ".//*[contains(@class,'contactinfo')]//a[@class='tel']//text()")
            self.load_date(
                " ".join(response.xpath(".//*[@id='overzicht']//div[@class='prop-descr']//text()").getall()),
                "",
                item_loader,
            )  # response.xpath(".//tr[th[.='Beschikbaar vanaf']]/td//text()").get()
            yield item_loader.load_item()

    def get_from_detail_panel(self, text, item_loader):
        """check all keywords for existing"""
        keywords = {
            "parking": [
                "parking",
                "garage",
                "parkeerplaatsen",
                "garagepoort",
                "parkeerplaats",
                "ondergrondse staanplaats",
            ],
            "balcony": ["balkon"],
            "pets_allowed": ["huisdieren toegelaten"],
            "furnished": ["gemeubileerd", "bemeubeld", "ingericht", "ingerichte", "gemeubeld"],
            "swimming_pool": ["zwembad"],
            "dishwasher": ["vaatwasser", "vaatwas", "afwasmachine"],
            "washing_machine": ["wasmachine"],
            "terrace": ["terras", "oriëntatie tuin/terras"],
            "elevator": ["lift", "elevator"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, True)

    def sub_string_between(self, source, s1, s2):
        tmp = source[source.index(s1) + len(s1) :]
        return tmp[: tmp.index(s2)]

    def get_lang(self):
        return {
            "Accept-Language": self.locale,
        }

    def load_date(self, description, data_txt, item_loader):
        """do some clean and format if need"""
        desc = description.casefold()
        if "geen huisdieren" in desc:
            item_loader.add_value("pets_allowed", False)
        date_list = [
            "beschikbaar vanaf",
            "vrij op",
            "vrij vanaf",
            "beschikbaarheid",
            "beschikbaar",
        ]
        for x in date_list:
            if x in desc or data_txt:
                available_date = re.search(r"(\d{2}[/-])?\d{2}[/-]\d{4}", desc.split(x)[-1][:20])
                if available_date:
                    available_date = available_date.group()
                else:
                    available_date = re.search(r"(\d{2}[/-])?\d{2}[/-]\d{4}", data_txt)
                    if available_date:
                        available_date = available_date.group()
                if available_date:
                    if len(available_date) == 7:
                        item_loader.add_value("available_date", format_date(available_date, "%m/%Y"))
                    else:
                        item_loader.add_value(
                            "available_date",
                            format_date(available_date, "%d/%m/%Y")
                            if "/" in available_date
                            else format_date(available_date, "%d-%m-%Y"),
                        )
