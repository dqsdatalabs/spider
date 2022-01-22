# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import re
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class ArcasaSpider(scrapy.Spider):
    name = "arcasa_disabled"
    allowed_domains = ["arcasa.be"]
    start_urls = ("http://www.arcasa.be/te-huur",)
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for link in response.css("#start .row").xpath("./div[@class]"):
            yield scrapy.Request(
                response.urljoin(link.xpath(".//a/@href").get()), self.parse_detail, headers=self.get_lang()
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

    def parse_detail(self, response):
        """parse detail page """
        main_block_xpath = ".//div[@class='main-wrapper']"
        main_block = response.xpath(main_block_xpath)
        intro = main_block.xpath(".//section[@class='projectintro']")
        stats = " ".join(main_block.xpath(".//section[@class='projectlist']//p[i]//text()").getall())
        foot = response.xpath(".//footer")
        if len(main_block) == 1:
            item_loader = ListingLoader(response=response)
            item_loader.add_value(
                "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
            )
            item_loader.add_value("external_link", response.url)
            item_loader.add_value(
                "external_id",
                re.search(r"[0-9]+", response.xpath(".//a[@id='ContentControl_lnkContact']/@href").get()).group(),
            )
            desc =  " ".join(response.xpath("//section[@class='projectintro']/div//p/text()").extract())
            if desc:
                item_loader.add_value("description", desc.strip())

            sqm = response.xpath("//p[contains(.,'Bewoonbare')]/span/strong/text()").get()
            if sqm:
                item_loader.add_value("square_meters", sqm.replace("m²", "").strip())

            item_loader.add_value(
                "city",
                main_block.xpath(
                    ".//div[@id='ContentControl_pnlTitle' and @class]/div[@class='row']/div[1]/h3[1]/span/text()"
                )
                .get()
                .strip(),
            )
            item_loader.add_value(
                "address",
                main_block.xpath(".//div[@id='ContentControl_pnlTitle' and @class]/div[@class='row']/div[1]/p/text()")
                .get()
                .strip(),
            )
            if (
                "huis"
                in main_block.xpath(
                    ".//div[@id='ContentControl_pnlTitle' and @class]/div[@class='row']/div[1]/h3[1]/text()"
                )
                .getall()[-1]
                .casefold()
            ):
                item_loader.add_value("property_type", "house")
            elif (
                "appartement"
                in main_block.xpath(
                    ".//div[@id='ContentControl_pnlTitle' and @class]/div[@class='row']/div[1]/h3[1]/text()"
                )
                .getall()[-1]
                .casefold()
            ):

                item_loader.add_value("property_type", "apartment")
            else:
                return

            item_loader.add_xpath(
                "rent_string",
                f"{main_block_xpath}//div[@id='ContentControl_pnlTitle' and @class]/div[@class='row']//h3[contains(.,'per maand')]/text()",
            )

            item_loader.add_value("title", response.xpath(".//meta[@property='og:title']/@content").get())

            # item_loader.add_value("description", intro.xpath(".//p/text()").get())
            item_loader.add_xpath("images", ".//a[@data-fancybox='gallery']/@href")
            self.get_general(item_loader)
            self.get_from_detail_panel(stats, item_loader)

            item_loader.add_value("landlord_name", "Arcasa BV")
            item_loader.add_value("landlord_email", "info@arcasa.be")
            item_loader.add_value("landlord_phone", "+32 (0) 3 877 70 70")
            yield item_loader.load_item()

    def get_general(self, item_loader):
        keywords = {
            "room_count": "Slaapkamers",
            "external_id": "Referentie",
            "bathroom_count": "Badkamers",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(k, f".//section[@class='projectlist']//p[contains(.,'{v}')]//text()")

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
