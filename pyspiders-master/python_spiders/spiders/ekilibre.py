# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request, FormRequest
from scrapy.selector import Selector
# from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re 
 

class MySpider(Spider):
    name = "ekilibre"
    allowed_domains = ["ekilibre.be"]
    start_urls = ("http://www.ekilibre.be/",)
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","
    external_source="Ekilibre_PySpider_belgium_fr"

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.ekilibre.be/fr/estates?filter={%22Category%22:[%22Appartement%22],%22TransactionStatus%22:[%22%C3%A0%20Louer%22]}&sort=CreatedOn|Desc",
                "property_type": "apartment",
            },
            {
                "url": "https://www.ekilibre.be/fr/estates?filter={%22Category%22:[%22Maison%22],%22TransactionStatus%22:[%22%C3%A0%20Louer%22]}&sort=CreatedOn|Desc",
                "property_type": "house",
            },
        ]

        for url in start_urls:
            yield Request(url=url.get("url"), callback=self.parse, meta={"response_url": url.get("url"), "page": 1,'property_type': url.get('property_type')})

    def parse(self, response):
        seen = False
        listings = response.xpath('//div[contains(@class,"estates-list-item")]//a/@href').extract()
        for url in listings:

            yield Request(
                url=response.urljoin(url),
                callback=self.parse_detail,
                meta={"response_url": response.urljoin(url), "property_type": response.meta.get("property_type")},
            )
            seen = True

        if len(listings) == 12 and seen:
            next_page_url = response.meta.get("response_url").split("&pageindex=")[0] + "&pageindex=" + str(response.meta.get("page") + 1)
            yield Request(
                url=next_page_url,
                callback=self.parse,
                meta={
                    "response_url": next_page_url,
                    "property_type": response.meta.get("property_type"),
                    "page": response.meta.get("page") + 1,
                },
            )

    def parse_map(self, response, item_loader):
        """ parse geo info"""
        geo = re.search(r"lat: \d+\.\d{3,}", response.text)
        geo1 = re.search(r"lng: \d+\.\d{3,}", response.text)
        if geo and geo1:
            geo = geo.group().split(":")
            geo1 = geo1.group().split(":")
            item_loader.addvalue("latitude", geo[-1].strip())
            item_loader.addvalue("longitude", geo1[-1].strip())
            # self.get_from_geo(item)

    def parse_detail(self, response):
        """parse detail page """
        main_block = response.css(".estate-details")
        if len(main_block) == 1:
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_id", find_external_id(response.url))
            item_loader.add_value(
                "external_source", self.external_source
            )
            item_loader.add_xpath("title", ".//aside//address//text()")
            item_loader.add_value("property_type", response.meta.get("property_type"))
            zipcode = response.xpath(".//aside//address//text()[last()]").get().strip()

            item_loader.add_value("zipcode", zipcode.split(" ")[0].strip())
            item_loader.add_value("city", " ".join(zipcode.split(" ")[1:]).strip())
            item_loader.add_xpath("address", ".//aside//address//text()")


            rent=response.xpath(".//aside//div[@class='card-body' and address]/div[2]/span/text()").get()
            if rent:
                rent=rent.replace("\xa0","")
                item_loader.add_value("rent_string", rent)
            item_loader.add_xpath("images", ".//a[@class='stretched-link']/@href")
            contact = ".//div[div[@class='ev-exposee-contact-details-name']]"
            item_loader.add_xpath("landlord_email", f"{contact}//li[@class='mail']/a/@href")

            desc = " ".join(response.xpath("//div[contains(@class,'estate-details--description')]/p//text()").getall())
            if desc:
                item_loader.add_value("description",desc.strip())

            item_loader.add_xpath("landlord_phone", f"{contact}//span[@itemprop='telephone']//text()")
            item_loader.add_xpath("landlord_name", f"{contact}//div[@class='ev-exposee-contact-details-name']//text()")

            dt = response.xpath(
                ".//*[contains(@class,'tab-content')]//li[span[contains(.,'Date de disponibilité')]]/span[2]//text()"
            ).get()
            if dt:
                date_parsed = dateparser.parse(dt.strip())
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            furnished=response.xpath("//span[.='Meublé']/following-sibling::span/text()").get()
            if furnished and "oui" in furnished.lower():
                item_loader.add_value("furnished",True)
            else:
                item_loader.add_value("furnished",False)
            item_loader.add_value("landlord_email", "info@ekilibre.be")
            item_loader.add_value("landlord_phone", "087 31 63 00")
            item_loader.add_value("landlord_name", "Ekilibre")
            self.parse_map(response, item_loader)
            self.get_from_detail_panel(
                " ".join(
                    response.xpath(
                        f".//*[contains(@class,'tab-content')]//li[span[2][contains(.,'Non')]]/span[1]//text()"
                    ).getall()
                ),
                item_loader,
                bool_value=False,
            )
            self.get_from_detail_panel(
                " ".join(
                    response.xpath(
                        f".//*[contains(@class,'tab-content')]//li[span[2][contains(.,'Oui')]]/span[1]//text()"
                    ).getall()
                ),
                item_loader,
            )
            self.get_general(item_loader)
            yield item_loader.load_item()

    def get_general(self, item_loader):
        keywords = {
            "external_id": "Référence",
            "floor": "Nombre d'étages",
            "utilities": "Charges mensuelles",
            # "available_date": "Date de disponibilité",
            "room_count": "Nombre de chambres",
            "bathroom_count": "Nombre de salle de bain",
            "square_meters": "Surface habitable",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(
                k, f'.//*[contains(@class,"tab-content")]//li[span[contains(.,"{v}")]]/span[2]//text()'
            )

    def get_from_detail_panel(self, text, item_loader, bool_value=True):
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
            "elevator": ["ascenseur", "elevator"],
        }

        #value = remove_white_spaces(text).casefold()
        value = text.strip()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, bool_value)

def find_external_id(external_link):
    result = external_link.split("/")[-1].split("-")[1]
    return result



