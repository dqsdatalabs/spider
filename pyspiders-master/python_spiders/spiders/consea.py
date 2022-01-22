# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import json
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import re


class VastgoeddemeyerSpider(scrapy.Spider):
    name = "consea"
    allowed_domains = ["consea.immo"]
    start_urls = (
        "https://www.consea.immo/te-huur?searchon=list&transactiontype=Rent&sorts=Dwelling&transactiontype=Rent",
        "https://www.consea.immo/te-huur?searchon=list&transactiontype=Rent&sorts=Flat&transactiontype=Rent",
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
            address = response.xpath("//h1/text()[1]").get()
            if address:
                address = re.sub('\s{2,}', ' ', address)
                item_loader.add_value("address", address)
                item_loader.add_value("city", " ".join(address.split(",")[-1].strip().split(" ")[1:]))
                item_loader.add_value("zipcode", address.split(",")[-1].strip().split(" ")[0])

            script_map = response.xpath("//script[contains(.,'latitude:')]/text()").get()
            if script_map:
                item_loader.add_value("latitude", script_map.split("latitude:")[-1].split(",")[0].strip())
                item_loader.add_value("longitude", script_map.split("longitude:")[-1].split(",")[0].strip())
            # logging.debug(detail_node.get())
            item_loader.add_xpath("description","//div[@id='description']//p//text()")

            item_loader.add_xpath("images", ".//a[@class='gallery']/@href")
            room = response.xpath("//tr[td[.='Slaapkamers:']]/td[2]/text()[.!='Ja']").extract_first()
            if room:
                item_loader.add_xpath("room_count", room)

            bedroom = response.xpath("//tr[td[.='Badkamers:']]/td[2]/text()[.!='Ja']").extract_first() 
            if bedroom:
                item_loader.add_xpath("bathroom_count", bedroom)

            item_loader.add_xpath(
                "rent_string", f"{detail_node_xpath}//tr[td[@class='kenmerklabel' and contains(.,'Prijs')]]/td[2]"
            )
            # self.get_by_keywords(item, self.get_from_detail_panel(detail_node))
            item_loader.add_value(
                "landlord_phone",
                "050 72 92 60"
            )
            item_loader.add_value(
                "landlord_email",
                "immo.maldegem@consea.be"
            )
            item_loader.add_value("landlord_name", "Consea")
            item_loader.add_xpath(
                "floor", ".//tr[td[@class='kenmerklabel' and .='Op verdieping:']]/td[@class='kenmerk']/text()"
            )

            utilities = response.xpath("//table//tr[td[contains(.,'kosten')]]/td[2]/text()").get()
            if utilities:
                utilities = utilities.split("€")[1].split("/")[0].strip()
                item_loader.add_value("utilities", utilities)

            meters = response.xpath("//table//tr[td[.='Bewoonbare opp.:']]/td[2]/text()").extract_first()
            if meters:
                s_meters = meters.split("m²")[0].strip()
                if s_meters !="0":
                    item_loader.add_value("square_meters",s_meters )
                    
            item_loader.add_xpath(
                "square_meters", ".//tr[td[@class='kenmerklabel' and .='Op verdieping:']]/td[@class='kenmerk']/text()"
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

            park = response.xpath(".//tr[td[@class='kenmerklabel' and .='Garage:']]/td[@class='kenmerk']/text()").get()
            park2 = response.xpath(
                ".//tr[td[@class='kenmerklabel' and .='Parking:']]/td[@class='kenmerk']/text()"
            ).get()
            if (park and park != "0") or (park2 and park2 != "0"):
                item_loader.add_value("parking", True)

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
