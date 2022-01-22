# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import json
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class VastgoedfransenSpider(scrapy.Spider):
    name = "vastgoedfransen"
    allowed_domains = ["vastgoedfransen.be"]
    start_urls = start_urls = (
        "https://www.vastgoedfransen.be/nl/te-huur?searchon=list&transactiontype=Rent&sorts=Dwelling&transactiontype=Rent",
        "https://www.vastgoedfransen.be/nl/te-huur?searchon=list&transactiontype=Rent&sorts=Flat&transactiontype=Rent",
    )
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, headers=self.get_lang())

    def parse(self, response, **kwargs):
        for link in response.xpath(".//a[contains(@title,'Meer info')]"):
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
        geo = response.xpath(".//*[@type='application/ld+json']/text()").get()
        geo = json.loads(geo)
        if geo:
            # geo=geo.group()[1:-2].split(",")
            item_loader.add_value("title", geo["name"])
            item_loader.add_value("external_id", geo["id"].split("/")[-1])
            item_loader.add_value("description", geo["description"])
            item_loader.add_value("address", geo["geo"]["address"]["streetAddress"])
            item_loader.add_value("city", geo["geo"]["address"]["addressRegion"])
            item_loader.add_value("zipcode", geo["geo"]["address"]["postalCode"])
            item_loader.add_value("latitude", geo["geo"]["latitude"])
            item_loader.add_value("longitude", geo["geo"]["longitude"])
            for item in geo["additionalProperty"]:
                if item["name"] == "Slaapkamers":
                    item_loader.add_value("room_count", item["value"])
                elif item["name"] == "Badkamers":
                    item_loader.add_value("bathroom_count", item["value"])
                elif item["name"] == "Bewoonbare opp.":
                    item_loader.add_value("square_meters", item["value"])

        # else:
        #     logging.debug(response.text)

    def parse_detail(self, response, property_type):
        main_block_xpath = ".//div[@id='sb-site']"
        main_block = response.xpath(main_block_xpath)
        if len(main_block) == 1:
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url)
            item_loader.add_value(
                "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
            )
            item_loader.add_value("property_type", property_type)
            detail_node_xpath = ".//div[@id='characterisation']"
            # logging.debug(detail_node.get())
            self.parse_map(response, item_loader)
            item_loader.add_xpath("description", f"{detail_node_xpath}//div[@id='description']//p//text()")

            item_loader.add_xpath("images", ".//a[@class='gallery']/@href")

        utilities = "".join(response.xpath("//tr[td[.='Totale kosten:']]/td[2]/text()").extract())
        if utilities:
            if "/" in utilities:
                uti = utilities.split("/")[0].strip()
                item_loader.add_value("utilities",uti.strip()) 
            else:
               item_loader.add_value("utilities",utilities.strip()) 

        parking = "".join(response.xpath("//tr[td[.='Parking:']]/td[2]/text()").extract())
        if parking:
            if "0" not in parking:
                item_loader.add_value("parking",True) 
            

            item_loader.add_xpath(
                "rent_string", f"{detail_node_xpath}//tr[td[@class='kenmerklabel' and contains(.,'Prijs')]]/td[2]"
            )
            # self.get_by_keywords(item, self.get_from_detail_panel(detail_node))
            item_loader.add_value(
                "landlord_phone",
                response.xpath(f"{main_block_xpath}//a[@class='icon-whatsapp']/@href").get().split("=")[-1],
            )
            item_loader.add_xpath("landlord_email", ".//*[@data-ga-id='link_email']/@title")
            item_loader.add_value("landlord_name", "Real estate Fransen & Co")
            item_loader.add_xpath(
                "floor", ".//tr[td[@class='kenmerklabel' and .='Op verdieping:']]/td[@class='kenmerk']/text()"
            )
            item_loader.add_xpath(
                "external_id", ".//tr[td[@class='kenmerklabel' and .='Referentie:']]/td[@class='kenmerk']/text()"
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

            self.get_from_detail_panel(
                " ".join(
                    response.xpath(".//tr[td[@class='kenmerk' and .='Ja']]/td[@class='kenmerklabel']//text()").getall()
                ),
                item_loader,
            )
            yield item_loader.load_item()

    def get_lang(self):
        return {
            "Accept-Language": self.locale,
        }

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
