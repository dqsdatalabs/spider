# -*- coding: utf-8 -*-
# Author:
import re
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class RovacSpider(scrapy.Spider):
    name = "rovac"
    allowed_domains = ["rovac.be"]
    start_urls = (
        "https://www.rovac.be/te-huur?sorts=Flat&&price-from=&price-to=",
        "https://www.rovac.be/te-huur?sorts=Dwelling&price-from=&price-to=",
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
        """parse list page and send requests to detail page, read fields if exist """
        for link in response.xpath(".//section[contains(@class,'container pubs')]/figure/a"):
            # print(link.get())
            if link.xpath(".//*[contains(@class,'label sold')]").get():
                pass
            else:
                yield scrapy.Request(
                    response.urljoin(link.xpath("@href").get()),
                    self.parse_detail,
                    cb_kwargs=dict(property_type="house" if "Dwelling" in response.url else "apartment"),
                )

        yield from self.parse_next(response)

    def parse_next(self, response):
        """ parse next page"""
        xpath = ".//a[@title='Volgende']"
        for link in response.xpath(xpath):
            yield response.follow(link, self.parse)

    def parse_map(self, response, item_loader):
        """ parse geo info"""
        geo = re.search(r"\d+\.\d{5,},\d+\.\d{5,}", response.text)
        if geo:
            geo = geo.group().split(",")
            item_loader.add_value("latitude", geo[0])
            item_loader.add_value("longitude", geo[1])

    def parse_detail(self, response, property_type):
        """parse detail page """
        main_block_xpath = ".//*[contains(@class,'renderbody')]"
        main_block = response.xpath(main_block_xpath)

        if len(main_block) == 1:
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url)
            item_loader.add_value(
                "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
            )
            detail_node_xpath = ".//div[contains(@class,'characterisations')]"
            item_loader.add_value("property_type", property_type)

            item_loader.add_xpath("title", f"{main_block_xpath}//div[div[@class='social']]/h1/text()")
            self.parse_map(response, item_loader)
            item_loader.add_xpath("description", f"{main_block_xpath}//div[div[@id='icons-summary']]/p[1]//text()")

            item_loader.add_xpath("images", ".//a[@class='gallery']/@href")

            item_loader.add_xpath("rent_string", f"{detail_node_xpath}//tr[td[.='Prijs:']]/td[2]/text()")

            utilities = response.xpath("//td[contains(.,'Totale kosten')]/following-sibling::td/text()").get()
            if utilities:
                item_loader.add_value("utilities", utilities.split('€')[-1].split('/')[0].strip().replace(' ', ''))

            city = " ".join(response.xpath("//td[contains(.,'Adres:')]/following-sibling::td/text()").getall()).strip()
            if city:
                item_loader.add_value("zipcode", city.strip().split(' ')[-2].strip())
                item_loader.add_value("city", city.strip().split(' ')[-1].strip())

            # self.get_by_keywords(item, self.get_from_detail_panel(detail_node))
            item_loader.add_xpath("landlord_email", ".//i[contains(@class,'icon-mail')]//text()")
            item_loader.add_xpath("landlord_phone", ".//a[contains(@href,'tel:')]/@href")
            item_loader.add_value("landlord_name", "Rovac Immobiliën")
            self.get_from_detail_panel(
                " ".join(
                    response.xpath(".//tr[td[@class='kenmerk' and .='Ja']]/td[@class='kenmerklabel']//text()").getall()
                ),
                item_loader,
                response,
            )
            yield item_loader.load_item()

    def get_from_detail_panel(self, text, item_loader, response):
        item_loader.add_xpath(
            "square_meters",
            ".//tr[td[@class='kenmerklabel' and  contains( .,'Bewoonbare opp.:')]]/td[@class='kenmerk']/text()",
        )
        item_loader.add_xpath(
            "floor", ".//tr[td[@class='kenmerklabel' and  contains( .,'Op verdieping:')]]/td[@class='kenmerk']/text()"
        )
        item_loader.add_xpath(
            "address", ".//tr[td[@class='kenmerklabel' and  contains( .,'Adres:')]]/td[@class='kenmerk']/text()"
        )
        item_loader.add_xpath(
            "external_id", ".//tr[td[@class='kenmerklabel' and contains( .,'Referentie:')]]/td[@class='kenmerk']/text()"
        )
        item_loader.add_xpath(
            "room_count",
            ".//tr[td[@class='kenmerklabel' and contains( .,'Slaapkamers:')]]/td[@class='kenmerk']/text()",
        )
        item_loader.add_xpath(
            "bathroom_count",
            ".//tr[td[@class='kenmerklabel' and contains( .,'Badkamers:')]]/td[@class='kenmerk']/text()",
        )
        available_date = response.xpath(
            ".//tr[td[@class='kenmerklabel' and .='Beschikbaar vanaf:']]/td[@class='kenmerk']/text()"
        ).get()
        if available_date and available_date != format_date(available_date):
            item_loader.add_value(
                "available_date",
                format_date(available_date),
            )
        pets_allowed = response.xpath(
            ".//tr[td[@class='kenmerklabel' and .='Huisdieren toegelaten:']]/td[@class='kenmerk']/text()"
        ).get()
        if pets_allowed:
            item_loader.add_value(
                "pets_allowed",
                pets_allowed != "Neen",
            )

        park = response.xpath(".//tr[td[@class='kenmerklabel' and .='Garage:']]/td[@class='kenmerk']/text()").get()
        park2 = response.xpath(".//tr[td[@class='kenmerklabel' and .='Parking:']]/td[@class='kenmerk']/text()").get()
        if (park and park != "0") or (park2 and park2 != "0"):
            item_loader.add_value("parking", True)
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