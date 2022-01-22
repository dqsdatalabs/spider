# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class ImmogrysonSpider(scrapy.Spider):
    name = "immocrevits"
    external_source='Immocrevits_PySpider_belgium_nl'
    allowed_domains = ["crevits.be"]
    start_urls = [
        "https://www.crevits.be/nl/te-huur/woningen/",
        "https://www.crevits.be/nl/te-huur/appartementen/",
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
        for item_responses in response.xpath(".//div[contains(@class,'spotlight property')]"):
            link = item_responses.xpath(".//a")
            if link and not item_responses.xpath(".//div[contains(@class,'property-sticker') and contains(.,'verhuurd')]"):
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
                "external_source", self.external_source)
            
            item_loader.add_value("property_type", cate)
            header_node = response.xpath("//div[@class='property__header-block']")
            detail_node = response.xpath(".//div[@class='property__details']")
            item_loader.add_value(
                "external_id", header_node.xpath("substring-after(//*[contains(@class,'property__header-block__ref')]//text(),':')").get()
            )
            item_loader.add_value("title", main_block.xpath("//head/title/text()").get())

            item_loader.add_value(
                "address",
                " ".join(
                    header_node.xpath(
                        ".//*[contains(@class,'property__header-block__adress__street')]//text()"
                    ).getall()
                ),
            )
            deposit = header_node.xpath("substring-before(//div[@id='sectionFinancial']//tr[td[contains(.,'Huurwaarborg')]]/td[2]//text(),',')").get()
            if deposit:
                item_loader.add_value("deposit", deposit.strip())

            available_date = header_node.xpath("//tr[@class='even']//td[contains(.,'Vrij op')]//following-sibling::td[contains(.,'/')]//text()").get()
            if available_date:
                item_loader.add_value("available_date", available_date)

            utilities = response.xpath("//tr[@class='odd']//td[contains(.,'Maandelijks')]//following-sibling::td//text()").get()
            if utilities:
                item_loader.add_value("utilities", utilities.strip())
            else:
                utilities = response.xpath("//tr[@class='odd']//td[contains(.,'Provisie')]//following-sibling::td//text()").get()
                if utilities:
                    item_loader.add_value("utilities", utilities.strip())

            city = header_node.xpath("substring-after(//*[contains(@class,'property__header-block__adress__street')]//text(),', ')").get()
            if city:
                item_loader.add_value("zipcode", city.split(" ")[0].strip())
                item_loader.add_value("city", " ".join(city.split(" ")[1:]).strip())
            item_loader.add_value(
                "description",
                " ".join(detail_node.xpath(".//div[@class='property__details__block__description']//text()").getall()),
            )
            floor = response.xpath("//table//tr[td[contains(.,'Verdieping')]]/td[2]//text()").get()
            if floor:
                item_loader.add_value("floor", floor.strip())
            bathroom_count = response.xpath("//table//tr[td[contains(.,'Aantal badkamers')]]/td[2]//text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.strip())
            else:
                bathroom_count = response.xpath("substring-after(//table//tr/td[@class='label' and contains(.,'Badkamer') and not(contains(.,'type'))],'Badkamer')").get()
                if bathroom_count:
                    item_loader.add_value("bathroom_count", bathroom_count.strip())
            item_loader.add_value("currency", "EUR")
            item_loader.add_xpath("latitude", ".//div[@id='pand-map']/@data-geolat")
            item_loader.add_xpath("longitude", ".//div[@id='pand-map']/@data-geolong")

            item_loader.add_xpath("images", ".//div[@id='pand-carousel']//img/@data-src")
            item_loader.add_xpath("rent", "//tr[@class='even']//td[contains(.,'Prijs')]//following-sibling::td//text()")
            square_meters = detail_node.xpath("substring-before(//div[@id='sectionConstruction']//tr[td[contains(.,'Woonoppervlakte')]]/td[2]//text(),'m')").get()
            if not square_meters:
                square_meters = detail_node.xpath("substring-before(//div[@id='sectionConstruction']//tr[td[contains(.,'Oppervlakte') or contains(.,'oppervlakte')]]/td[2]//text(),'m')").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters)

            self.get_general(item_loader)
            self.get_from_detail_panel(
                " ".join(main_block.xpath("//tr[td[@class='label']]//text()").getall()), item_loader
            )

            item_loader.add_value("landlord_phone", "32 9 222 27 76")
            item_loader.add_value("landlord_email", "immo@crevits.be")
            item_loader.add_value("landlord_name", "IMMOBILIËN CREVITS")
            yield item_loader.load_item()

    def get_general(self, item_loader):
        keywords = {
            "room_count": "Aantal slaapkamers",
            # "bathroom_count": "Aantal badkamers",
            "landlord_name": "Kantoor",
            "landlord_phone": "Tel. nr",
            # "utilities": "Maandelijks",
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
