# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import *


class ImmogrysonSpider(scrapy.Spider):
    """
    get available_date is possible
    """

    name = "immogryson"
    allowed_domains = ["immogryson.be"]
    start_urls = [
        "https://www.immogryson.be/nl/te-huur/woningen/",
        "https://www.immogryson.be/nl/te-huur/appartementen/",
    ]
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, headers=self.get_lang())

    def parse(self, response, **kwargs):
        """parse list page and send requests to detail page, read fields if exist """
        for link in response.xpath(".//div[@class='spotlight']//a"):
            yield scrapy.Request(
                response.urljoin(link.xpath("@href").get()),
                self.parse_detail,
                cb_kwargs=dict(cate="house" if "woningen" in response.url else "apartment"),
            )

    def parse_detail(self, response, cate):
        """parse detail page """
        main_block = response.xpath(".//div[@id='below-carousel']")
        if len(main_block) == 1:
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url)
            item_loader.add_value(
                "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
            )
            item_loader.add_value("property_type", cate)
            # tmp = main_block.xpath(".//div[@id='desc']")
            header_node = response.xpath("//div[@class='property__header-block']")
            detail_node = response.xpath(".//div[@class='property__details']")
            item_loader.add_value(
                "external_id", header_node.xpath(".//*[contains(@class,'property__header-block__ref')]//text()").get()
            )
            item_loader.add_value("title", main_block.xpath("//head/title/text()").get())

            address = response.xpath("//*[contains(@class,'property__header-block__adress__street')]//text()").get()
            if address:
                item_loader.add_value("address", address.strip())
                zipcode = address.split(",")[-1].strip().split(" ")[0]
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("city", address.strip().split(" ")[-1])
                
            item_loader.add_value(
                "description",
                " ".join(detail_node.xpath(".//div[@class='property__details__block__description']//text()").getall()),
            )
            item_loader.add_xpath("latitude", ".//div[@id='pand-map']/@data-geolat")
            item_loader.add_xpath("longitude", ".//div[@id='pand-map']/@data-geolong")
            rent = response.xpath(".//h2[@class='property__sub-title']//text()").get()
            if not rent:
                rent = response.xpath("//h1//text()[contains(.,'€')]").get()
            if rent:
                item_loader.add_value("rent_string", rent)
            
            item_loader.add_xpath("images", ".//div[@id='pand-carousel']//img/@data-src")
            
            item_loader.add_xpath(
                "square_meters", detail_node.xpath(".//div[span[@class='icon-layers']]/span[2]").get()
            )

            self.get_general(item_loader)
            self.get_from_detail_panel(
                " ".join(main_block.xpath("//tr[td[@class='label']]//text()").getall()), item_loader
            )
            self.load_date(
                " ".join(detail_node.xpath(".//div[@class='property__details__block__description']//text()").getall()),
                "",
                item_loader,
            )

            item_loader.add_value("landlord_email", "info@immogryson.be")

            yield item_loader.load_item()

    def get_general(self, item_loader):
        keywords = {
            "room_count": "Aantal slaapkamers",
            "bathroom_count": "Aantal badkamers",
            "landlord_name": "Kantoor",
            "landlord_phone": "Tel. nr",
            "utilities": "Maandelijks",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(k, f'.//tr[td[contains(.,"{v}")]]/td[2]//text()')

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
            # "furnished": ["gemeubileerd", "bemeubeld", "ingericht", "ingerichte", "gemeubeld"],
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

    def get_lang(self):
        return {
            "Accept-Language": self.locale,
        }

    def load_date(self, description, data_txt, item_loader):
        """do some clean and format if need"""
        desc = description.casefold()
        date_list = [
            "beschikbaar vanaf",
            "vrij op",
            "vrij",
            "vrij vanaf",
            "beschikbaarheid",
            "beschikbaar",
        ]
        month_keys = {
            r"(januari|jan\.?)": "1",
            r"(februari|febr\.?)": "2",
            r"(maart|mrt\.?)": "3",
            r"(april|apr\.?)": "4",
            r"mei": "5",
            r"juni": "6",
            r"juli": "7",
            r"(augustus|aug\.?)": "8",
            r"(september|sep\.?)": "9",
            r"(oktober|okt\.?)": "10",
            r"(november|nov\.?)": "11",
            r"(december|dec\.?)": "12",
        }
        for x in date_list:
            if x in desc or data_txt:
                available_date = re.search(r"\d{1,2}\s+\w+\s+\d{4}", desc.split(x)[-1][:20])
                if available_date:
                    available_date = available_date.group().casefold()
                    for k, v in month_keys.items():
                        available_date = re.sub(k, v, available_date)
                    item_loader.add_value("available_date", format_date(available_date, "%d %m %Y"))
