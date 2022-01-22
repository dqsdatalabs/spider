# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import re
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class VastgoedkantoordevrieseSpider(scrapy.Spider):

    name = "vastgoedkantoordevriese"
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","
    allowed_domains = ["vastgoedkantoordevriese.be"]
    start_urls = [
        "https://www.vastgoedkantoordevriese.be/te-huur/woningen",
        "https://www.vastgoedkantoordevriese.be/te-huur/appartementen",
    ]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, dont_filter=True, headers=self.get_lang())

    def parse(self, response, **kwargs):
        """parse list page and send requests to detail page, read fields if exist """
        # self.keys_words.add_exclude_words(["garage types"])

        for link in response.xpath(".//article"):
            item = dict()
            item["city"] = link.xpath(".//*[@class='spnCity']/text()").get()
            item["address"] = link.xpath(".//*[@class='spnStreet']/text()").get()
            item["currency"] = link.xpath(".//*[contains(@class,'spnPrice')]/span/text()").get()
            item["property_type"] = "apartment" if "appartementen" in response.url else "house"
            yield scrapy.Request(
                response.urljoin(link.xpath("./a/@href").get()),
                self.parse_detail,
                headers=self.get_lang(),
                cb_kwargs=dict(item=item),
            )
        yield from self.parse_next(response)

    def parse_next(self, response):
        """parse next page """
        xpath = ".//div[@class='nextPage']/a"
        for link in response.xpath(xpath):
            yield response.follow(
                link,
                self.parse,
                headers=self.get_lang(),
            )

    def parse_detail(self, response, item):
        """parse detail page """
        main_block = response.xpath(".//main[@role='main']")

        if len(main_block) == 1:
            item_loader = ListingLoader(response=response)
            rented = response.xpath("//label[contains(.,'Verhuurd')]/text()").get()
            if rented:
                return
            item_loader.add_value(
                "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
            )
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("city", item["city"])
            item_loader.add_value("address", item["address"])
            item_loader.add_value("property_type", item["property_type"])
            rent = item["currency"]
            item_loader.add_value("rent", rent.replace(" ","").strip().replace("\xa0",""))
            item_loader.add_value("currency", "EUR")

            item_loader.add_value("title", main_block.xpath(".//div[span[@class='spnPrice']]/h1//text()").get())
            utilities = response.xpath("//div[@class='divTxt']//text()[contains(.,'Huurprijs') and contains(.,'+')]").get()
            if utilities:
                item_loader.add_value("utilities",utilities.split("+")[1].split("EUR")[0])
            zip_code = re.findall(r'"postalCode":"\d{4}"', response.text)
            if zip_code:
                item_loader.add_value("zipcode", self.sub_string_between(zip_code[0], '":"', '"'))
            square = response.xpath("//div[section[@id='general']]//tr[td[contains(.,'Perceeloppervlakte')]]/td[@class='kenmerk']//text()").get()
            if not square:
                square = response.xpath("//div[section[@id='general']]//tr[td[contains(.,'Bewoonbare opp')]]/td[@class='kenmerk']//text()").get()
            if square:
                item_loader.add_value("square_meters",square.strip().split(" ")[0])
            item_loader.add_value("description", main_block.xpath(".//div[@class='divTxt']//text()").get())
            item_loader.add_value("latitude", main_block.xpath("//div[@id='streetview']/@data-lat").get())
            item_loader.add_value("longitude", main_block.xpath("//div[@id='streetview']/@data-lng").get())
            item_loader.add_xpath("images", ".//div[@id='detailSlide']//img/@src")
            self.get_general(item_loader)
            self.get_from_detail_panel(
                " ".join(response.xpath(".//div[@class='divDetailIcons']//label/text()").getall()), item_loader
            )

            item_loader.add_value("landlord_name", "Devriese vastgoedkantoor")
            item_loader.add_value("landlord_email", "info@vastgoedkantoordevriese.be")
            item_loader.add_value("landlord_phone", "+32(0)56440369")
            self.load_date(" ".join(main_block.xpath(".//div[@class='divTxt']//text()").getall()), "", item_loader)

            yield item_loader.load_item()

    def get_general(self, item_loader):
        keywords = {
            # "square_meters": "Bewoonbare opp",
            "room_count": "Slaapkamers",
            "external_id": "Referentie",
            "bathroom_count": "Badkamers",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(
                k, f".//div[section[@id='general']]//tr[td[contains(.,'{v}')]]/td[@class='kenmerk']//text()"
            )

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