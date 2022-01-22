# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import json
import re
import scrapy
from ..loaders import ListingLoader
from ..helper import *

class IvaSpider(scrapy.Spider):
    name = "iva"
    allowed_domains = ["iva.be"]
    start_urls = (
        "https://www.iva.be/nl/te-huur/appartement/",
        "https://www.iva.be/nl/te-huur/huis/",
    )
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for item_responses in response.css("#results .prop-item"):
            link = item_responses.xpath("./a/@href").get()
            item = dict()
            item["room_count"] = item_responses.css(".info::text").get()
            item["price"] = item_responses.css(".price").get()
            item["property_type"] = "apartment" if "appartement" in response.url else "house"
            if item["price"]:
                yield scrapy.Request(response.urljoin(link), self.parse_detail, cb_kwargs=dict(item=item))
        yield from self.parse_next(response)

    def parse_next(self, response):
        """parse next page """
        xpath = './/div[@id="paging"]//li[@class="pg-previous"]/a/@href'
        if not xpath or response.xpath('.//div[@id="paging"]//li[@class="pg-previous"]/a/@data-page') == "1":
            return
        for link in response.xpath(xpath):
            yield response.follow(link, self.parse)

    def parse_map(self, response, item_loader):
        """parse geo related fields """
        tmp = response.css("script:contains(Map)").get()
        tmp = json.loads(tmp[tmp.index("=") + 1 :])
        for k, value in tmp["Location"].items():
            if "items" in k:
                tmp = value[0]
                item_loader.addvalue("latitude", str(tmp.get("lat")))
                item_loader.addvalue("longitude", str(tmp.get("lng")))
                break

    def parse_detail(self, response, item):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        item_loader.add_value("room_count", item["room_count"])
        # item_loader.add_value("rent_string", item["price"])
        main_block = response.xpath(".//section[@class='imp-info']")
        if len(main_block) == 1:

            # stats = main_block.xpath(".//div[@class='table-responsive']/table[@class='table table-sm table-striped']")
            # contacts = response.xpath(".//footer")
            banner_path = ".//section[@class='prop-header']"

            item_loader.add_xpath("external_id", f"{banner_path}//p/span/text()")
            # self.get_by_keywords(item, self.get_from_detail_panel(stats))
            # item_loader.add_xpath(
            #     "bathroom_count",
            #     ".//div[@class='table-responsive']/table[@class='table table-sm table-striped']//td[contains(., 'Badkamer')]//text()",
            # )
            item_loader.add_xpath("address", f"{banner_path}//p/text()")
            address = response.xpath(f"{banner_path}//p/text()").get()
            address = address.split(",")[-1].strip().split(" ")
            item_loader.add_value("zipcode", address[0])
            item_loader.add_value("city", " ".join(address[1:]))
            item_loader.add_xpath("title", f"{banner_path}//div[@class='title title-circle']/h1/text()")
            item_loader.add_xpath("description", ".//div[@class='propertyDescription']//div/div[1]/text()")
            js_var = response.xpath(
                ".//body[@class='propertydetail propertydetail']/main/script[@type='text/javascript']"
            )[0].get()
            item_loader.add_value(
                "latitude",
                re.search(
                    r"(\-?\d+(\.\d+)?)",
                    js_var[js_var.index("lat") :][: js_var[js_var.index("lat") :].index(";")],
                ).group(),
            )
            item_loader.add_value(
                "longitude",
                re.search(
                    r"(\-?\d+(\.\d+)?)",
                    js_var[js_var.index("lng") :][: js_var[js_var.index("lng") :].index(";")],
                ).group(),
            )

            item_loader.add_xpath("square_meters", ".//ul[@class='shortinfo']//text()[contains(.,'Oppervlakte:')]")
            if not item_loader.get_collected_values("square_meters"):
                square_meters = response.xpath("//text()[contains(.,'oppervlakte van')]").get()
                if square_meters:
                    item_loader.add_value("square_meters", "".join(filter(str.isnumeric, square_meters.strip())))

            from datetime import datetime
            from datetime import date
            import dateparser
            available_date = response.xpath("//text()[contains(.,'Beschikbaar vanaf')]").get()
            if available_date:
                date_parsed = dateparser.parse(available_date.split('vanaf')[1].split('.')[0].strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
                today = datetime.combine(date.today(), datetime.min.time())
                if date_parsed:
                    result = today > date_parsed
                    if result == True:
                        date_parsed = date_parsed.replace(year = today.year + 1)
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

            if not item_loader.get_collected_values("bathroom_count"):
                bathroom_count = response.xpath("//td[contains(.,'Badkamer')]").get()
                if bathroom_count:
                    item_loader.add_value("bathroom_count", '1')
            
            if not item_loader.get_collected_values("terrace"):
                terrace = response.xpath("//td[contains(.,'Terras')]").get()
                if terrace:
                    item_loader.add_value("terrace", True)

            if not item_loader.get_collected_values("property_type"):
                if 'appartement' in response.url:
                    item_loader.add_value("property_type", 'apartment')
                else:
                    item_loader.add_value("property_type", 'house')

            item_loader.add_xpath("utilities", ".//ul[@class='shortinfo']//text()[contains(.,'Maandelijkse lasten:')]")
            for k, v in self.get_from_detail_panel(
                response.xpath(".//div[@class='tblInd detg']//table[@class='table table-sm table-striped']")
            ):
                item_loader.add_value(k, v)
            self.get_general(item_loader)
            item_loader.add_xpath("images", ".//div[@class='pictBox']/a/@href")
            item_loader.add_xpath("rent_string", ".//section[@class='imp-info']//strong[@class='price']/text()")
            item_loader.add_xpath("landlord_phone", ".//footer//p[i[@class='fas fa-phone']]/text()")
            item_loader.add_value("landlord_name", "IVA Immobiliën")
            yield item_loader.load_item()

    def get_general(self, item_loader):
        keywords = {
            # "room_count": "Slaapkamer",
            "bathroom_count": "Badkamer",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(
                k,
                f'.//div[@class="tblInd detg"]//table[@class="table table-sm table-striped"]//tr/td[contains(.,"{v}")]//text()',
            )

    def get_from_detail_panel(self, node):
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
        for sub_node in node.xpath(".//tr/td[1]"):
            title = sub_node.xpath("./text()").get()
            value = remove_white_spaces(title).casefold()
            for k, v in keywords.items():
                if any(s in value for s in v):
                    yield k, True