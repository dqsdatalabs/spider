# -*- coding: utf-8 -*-
# Author:
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class EngelvoelkerspavillondixellesSpider(scrapy.Spider):
    name = "engelvoelkersPavillondixelles"
    allowed_domains = ["engelvoelkers.com"]
    start_urls = (
        "https://www.engelvoelkers.com/fr/search/?remember=true&q=&startIndex=0&pageSize=18&facets=cntry%3Abelgium%3Brgn%3Abrussels_surroundings%3Bbsnssr%3Aresidential%3Btyp%3Arent%3Bobjcttyp%3Acondo",
        "https://www.engelvoelkers.com/fr/search/?remember=true&q=&startIndex=0&pageSize=18&facets=cntry%3Abelgium%3Brgn%3Abrussels_surroundings%3Bbsnssr%3Aresidential%3Btyp%3Arent%3Bobjcttyp%3Ahouse",
    )
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = ","
    scale_separator = "."

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for link in response.xpath(".//a[contains(@class,'ev-property-container')]"):
            item = {
                "title": response.xpath(".//div[@class='ev-teaser-title']//text()").get(),
                # "room_count": response.xpath(".//div[img[@title='Chambres à coucher']]//span//text()").get(),
                # "bathroom_count": response.xpath(".//div[img[@title='Salles de bains']]//span//text()").get(),
                "square_meters": response.xpath(".//div[img[@title='Surface Habitable']]//span//text()").get(),
                # "rent": response.xpath(".//div[@class='ev-teaser-price']/div[@class='ev-value']//text()").get(),
                "property_type": "house" if "house" in response.url else "apartment",
                "currency": "EUR",
            }

            # if item["rent"]:
            yield scrapy.Request(
                response.urljoin(link.xpath("@href").get()),
                self.parse_detail,
                cb_kwargs=dict(item=item),
            )
        yield from self.parse_next(response)

    def parse_next(self, response):
        """ parse next page"""
        xpath = ".//a[@class='ev-pager-next']"
        if response.xpath(xpath).get():
            for link in response.xpath(xpath):
                yield response.follow(link)

    def parse_detail(self, response, item):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        for k, v in item.items():
            item_loader.add_value(k, v)
        item_loader.add_xpath("description", ".//p[@itemprop='description']//text()")
        item_loader.add_xpath("images", ".//div[@id='keyVisual']//a/@href")
        item_loader.add_xpath("landlord_phone", ".//div[@class='ev-exposee-contact-details-content']//span[@itemprop='telephone']//text()")
        item_loader.add_xpath("landlord_name", ".//div[@class='ev-exposee-contact-details-name']//text()")
        item_loader.add_xpath(
            "landlord_email", ".//div[@class='ev-imprint-text']//text()[contains(.,'@engelvoelkers')]"
        )
        item_loader.add_xpath("external_id", ".//input[@name='displayID']/@value")
        self.get_from_detail_panel(
            " ".join(response.xpath(".//li[@class='ev-exposee-detail-fact']//text()").getall()), item_loader
        )
        rent=response.xpath("//div[@class='ev-key-fact-value']/span[@itemprop='price']/text()[.!=' (sur demande) ']").get()
        if rent and rent != "0":
            item_loader.add_value("rent", rent.replace(",","").replace(".","").strip())
            item_loader.add_value("currency", "EUR")
        address = response.xpath("substring-after(//div[contains(@class,'ev-exposee-subtitle')]/text(),'|')").get()
        if address:
            item_loader.add_value("address", address.split(",")[-1].strip())
            city = address.split(",")[-1].split("(")[0].strip()
            item_loader.add_value("city", city )
        room_count=response.xpath("//div[contains(@class,'ev-key-fact')]/div[contains(.,'Zimmer') or contains(.,'Chambres') or contains(.,'Pièce')]/div[contains(@class,'value')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count=response.xpath("//div[contains(@class,'ev-key-fact')]/div[contains(.,'Zimmer') or contains(.,'bains') or contains(.,'Pièce')]/div[contains(@class,'value')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        utilities = response.xpath("//ul[contains(@class,'ev-exposee-detail-facts')]/li[contains(.,'Charges')]/span/text()").get()
        if utilities:
            utilities = utilities.replace(",00","").strip()
            item_loader.add_value("utilities", utilities)
        
        # item_loader.add_xpath(
        #     "utilities", ".//span[@class='ev-exposee-detail-fact-value' and contains(.,'Charges')]//text()"
        # )
        yield item_loader.load_item()

    def get_from_detail_panel(self, text, item_loader):
        """check all keywords for existing"""
        keywords = {
            "parking": [
                "parking",
                "garage",
                "car",
                "aantal garage",
            ],
            "balcony": [
                "balcon",
                "nombre de balcon",
                "Nombre d",
                "balcony",
                "balcon arrière",
            ],
            "pets_allowed": ["animaux"],
            "furnished": ["meublé", "appartement meublé", "meublée"],
            "swimming_pool": ["piscine"],
            "dishwasher": ["lave-vaisselle"],
            "washing_machine": ["machine à laver", "lave linge"],
            "terrace": ["terrasse", "terrasse de repos", "terras"],
            "elevator": ["ascenseur", "ascenceur"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, True)