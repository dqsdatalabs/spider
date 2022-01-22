# -*- coding: utf-8 -*-
# Author: Daniel Qian
import json
import scrapy
from scrapy.http import JsonRequest
from ..loaders import ListingLoader
from ..helper import *
import dateparser


class ImmotermoteSpider(scrapy.Spider):
    name = "immoTermote"
    allowed_domains = ["immo-termote.be"]
    start_urls = ("https://www.immo-termote.be/nl/te-huur/#Type=1",)
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","
    external_source = "Immotermote_PySpider_belgium_nl"

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, headers=self.get_lang())

    def parse(self, response, **kwargs):
        token = self.sub_string_between(response.text, "mediatoken =", "';").replace("'", "").strip()
        yield JsonRequest(
            url="https://www.immo-termote.be/Modules/ZoekModule/RESTService/SearchService.svc/GetPropertiesJSON/0/0",
            data={
                "bedrooms": "0",
                "CheckedTypes": "0",
                "City": "0",
                "CompanyID": "0",
                "CountryExclude": "0",
                "CountryInclude": "0",
                "currentPage": "0",
                "ExtraSQL": "0",
                "ExtraSQLFilters": "0",
                "FilterOutTypes": "",
                "homeSearch": "0",
                "investment": "false",
                "Language": "NL",
                "latitude": "0",
                "longitude": "0",
                "MaxBedrooms": "0",
                "MaxPrice": "0",
                "MaxSurface": "0",
                "MaxSurfaceGround": "0",
                "MediaID": "0",
                "menuIDUmbraco": "0",
                "MinBedrooms": "0",
                "MinPrice": "0",
                "MinSurface": "0",
                "MinSurfaceGround": "0",
                "NavigationItem": "0",
                "newbuilding": "false",
                "NumResults": "1000",
                "officeID": "0",
                "OrderBy": "1",
                "PageName": "0",
                "PriceClass": "0",
                "PropertyID": "0",
                "PropertyName": "0",
                "Radius": "0",
                "Region": "0",
                "ShowChildrenInsteadOfProject": "false",
                "ShowProjects": "false",
                "SliderItem": "0",
                "SliderStep": "0",
                "SortField": "1",
                "SQLType": "3",
                "StartIndex": "1",
                "state": "0",
                "Token": token,
                "Transaction": "2",
                "Type": "1",
                "useCheckBoxes": "false",
                "UsePriceClass": "false",
            },
            dont_filter=True,
            callback=self.after_post,
            method="POST",
            headers=self.get_lang(),
            cb_kwargs=dict(property_type="apartment"),
        )
        yield JsonRequest(
            url="https://www.immo-termote.be/Modules/ZoekModule/RESTService/SearchService.svc/GetPropertiesJSON/0/0",
            data={
                "bedrooms": "0",
                "CheckedTypes": "0",
                "City": "0",
                "CompanyID": "0",
                "CountryExclude": "0",
                "CountryInclude": "0",
                "currentPage": "0",
                "ExtraSQL": "0",
                "ExtraSQLFilters": "0",
                "FilterOutTypes": "",
                "homeSearch": "0",
                "investment": "false",
                "Language": "NL",
                "latitude": "0",
                "longitude": "0",
                "MaxBedrooms": "0",
                "MaxPrice": "0",
                "MaxSurface": "0",
                "MaxSurfaceGround": "0",
                "MediaID": "0",
                "menuIDUmbraco": "0",
                "MinBedrooms": "0",
                "MinPrice": "0",
                "MinSurface": "0",
                "MinSurfaceGround": "0",
                "NavigationItem": "0",
                "newbuilding": "false",
                "NumResults": "1000",
                "officeID": "0",
                "OrderBy": "1",
                "PageName": "0",
                "PriceClass": "0",
                "PropertyID": "0",
                "PropertyName": "0",
                "Radius": "0",
                "Region": "0",
                "ShowChildrenInsteadOfProject": "false",
                "ShowProjects": "false",
                "SliderItem": "0",
                "SliderStep": "0",
                "SortField": "1",
                "SQLType": "3",
                "StartIndex": "1",
                "state": "0",
                "Token": token,
                "Transaction": "2",
                "Type": "3",
                "useCheckBoxes": "false",
                "UsePriceClass": "false",
            },
            dont_filter=True,
            callback=self.after_post,
            method="POST",
            headers=self.get_lang(),
            cb_kwargs=dict(property_type="house"),
        )

    def after_post(self, response, property_type):
        """ parse json """
        json_data = json.loads(response.text)
        for obj in json_data:
            if obj["Property_URL"] and len(obj["Property_URL"]) > 2:
                obj["Property_Type_Value"] = property_type
                yield scrapy.Request(
                    "https://www.immo-termote.be/nl" + obj["Property_URL"],
                    self.parse_detail,
                    dont_filter=True,
                    cb_kwargs=dict(obj=obj),
                )

    def parse_detail(self, response, obj):
        main_block = response.xpath(".//section[@class='content trans_2']")
        if len(main_block) == 1:
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url)
            item_loader.add_value(
                "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
            )
            item_loader.add_value("external_id", obj["FortissimmoID"])

            item_loader.add_value("title", obj["Property_Title"])
            item_loader.add_value("description", obj["Property_Description"])
            item_loader.add_value("city", obj["Property_City_Value"])
            item_loader.add_value("zipcode", obj["Property_Zip"])
            item_loader.add_value("address", " ".join([obj["Property_Street"], obj["Property_Number"]]))
            item_loader.add_value("latitude", str(obj["Property_Lat"]))
            item_loader.add_value("longitude", str(obj["Property_Lon"]))
            item_loader.add_value("property_type", obj["Property_Type_Value"])
            item_loader.add_value("square_meters", obj["Property_Area_Build"])
            item_loader.add_value("room_count", int(obj["bedrooms"]))

            price = obj["Property_Price"].split(" ")
            if len(price) != 2:
                pass
            else:
                item_loader.add_value("rent", int(price[1].replace("\xa0", "")))
                item_loader.add_value("currency", "EUR")
            if obj["Property_Furniture"] != "0":
                item_loader.add_value("furnished", True)

            main_block_xpath = ".//section[@class='content trans_2']"
            item_loader.add_xpath("images", ".//a[@class='rsImg']/@href")
            if not item_loader.get_collected_values("images"):
                images = response.urljoin(response.xpath("//div/img[@class='img-responsive']/@src").get())
                if images:
                    item_loader.add_value("images", list(images))
            
            item_loader.add_xpath(
                "landlord_name", f"{main_block_xpath}//div[@class='infoSales']//div[@class='info']/span[1]//text()"
            )

            item_loader.add_xpath(
                "landlord_phone", f"{main_block_xpath}//div[@class='infoSales']//div[@class='info']//a/@href"
            )
            dt = response.xpath(f"//tr[td[contains(.,'Beschikbaar vanaf')]]/td[2]//text()").get()
            if dt:
                dt = dateparser.parse(dt)
                if dt:
                    item_loader.add_value(
                        "available_date",
                        dt.date().strftime("%Y-%m-%d"),
                    )
            item_loader.add_xpath("utilities", f"//tr[td[contains(.,'Maandelijkse lasten')]]/td[2]//text()")
            pet = response.xpath(f"//tr[td[contains(.,'Huisdieren toegelaten')]]/td[2]/text()").get()
            if pet:
                item_loader.add_value("pets_allowed", "Nee" not in pet)

            if response.xpath(f"//th[contains(.,'Terras')]").get():
                item_loader.add_value("terrace", True)

            if (
                response.xpath(f"//th[contains(.,'Parkeerplaats')]").get()
                or response.xpath(f"//th[contains(.,'Garage')]").get()
            ):
                item_loader.add_value("parking", True)

            bathroom_count = response.xpath("//th[contains(.,'Badkamer')]/text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split('x')[0].strip())

            item_loader.add_value("landlord_email", "desselgem@immo-termote.be")

            yield item_loader.load_item()

    def sub_string_between(self, source, s1, s2):
        tmp = source[source.index(s1) + len(s1) :]
        return tmp[: tmp.index(s2)]

    def get_lang(self):
        return {
            "Accept-Language": self.locale,
        }