# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import *
import re


class BruyereimmobilierSpider(scrapy.Spider):
    name = "bruyereImmobilier"
    allowed_domains = ["bruyere-immobilier.com"]
    start_urls = (
        "https://www.bruyere-immobilier.com/fr/locations/appartements",
        "https://www.bruyere-immobilier.com/fr/locations/maisons",
    )
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","
    external_source = "Bruyereimmobilier_PySpider_belgium_fr"
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for item_responses in response.css(".ads .ad").xpath(".//a"):
            link = item_responses.xpath(".//@href").get()
            if link:
                yield scrapy.Request(
                    response.urljoin(link),
                    self.parse_detail,
                    cb_kwargs=dict(property_type="house" if "maisons" in response.url else "apartment"),
                )

        yield from self.parse_next(response)

    def parse_next(self, response):
        """parse next page """
        xpath = ".//a[@title='Page suivante']/@href"
        for link in response.xpath(xpath).getall():
            yield response.follow(response.urljoin(link), self.parse)

    def parse_detail(self, response, property_type):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_xpath("title", ".//head/meta[@property='og:title']/@content")
        city="".join(response.xpath(".//head/meta[@property='og:title']/@content").get())
        if city:
            item_loader.add_value("city",city.split("-")[-1].strip())
        item_loader.add_xpath("description", ".//head/meta[@property='og:description']/@content")
        item_loader.add_xpath("images", ".//a[@rel='slideshow']/@href")
        item_loader.add_xpath("rent_string", ".//div[@class='title']/h2//text()")
        externalid=response.xpath(".//p[@class='comment']/span[@class='reference']/text()").get()
        if externalid:
            item_loader.add_xpath("external_id",externalid.split(".")[-1].strip())
        item_loader.add_xpath("landlord_name", ".//aside[h2[.='Contactez-nous']]/h4/text()")
        item_loader.add_xpath("landlord_phone", ".//aside[h2[.='Contactez-nous']]/p/a/text()")
        squarecheck=item_loader.get_output_value("square_meters")
        if not squarecheck:
            squ2="".join(response.xpath("//p[@class='comment']/span/..//text()").getall())
            squ=re.search(r"(\d+)\.(\d+)m²",squ2)
            if squ:
                squ=squ.group().split(".")[0]
                if squ:
                    item_loader.add_value("square_meters",squ)
            elif not squ:
                squ1=re.search(r"(\d+)\.(\d+)\s*(m²)",squ2)
                if squ1:
                    squ1=squ1.group().split(".")[0]
                    if squ1:
                        item_loader.add_value("square_meters",squ1)
                else:
                    squ3=re.search(r"(\d+)\s*(m²)",squ2)
                    if squ3:
                        squ3=squ3.group().split(".")[0]
                        if squ3:
                            item_loader.add_value("square_meters",squ3)


        deposit = response.xpath("//li/text()[contains(.,'Dépôt de garantie')]/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit", int(float(deposit.split('€')[0].strip())))
            
        else:
            deposit = response.xpath("//text()[contains(.,'Dépôt de garantie')]").get()
            if deposit:
                item_loader.add_value("deposit", int(float(deposit.split('€')[0].split(':')[-1].strip())))

        self.get_general(item_loader)
 
        if not item_loader.get_collected_values("room_count"):
            room_count = response.xpath("//p[@class='comment']//text()[contains(.,'chambre')]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split('chambre')[0].split(',')[-1].strip())

        address = response.xpath('/html/head//meta[contains(@content, "Location")]/@content').get()
        if address:
            item_loader.add_value('address', ' '.join(address.split(",")[2:]))
        utilities="".join(response.xpath("//p[@class='comment']/span/..//text()").getall())
        if utilities:
            uti=utilities.split("Loyer")[-1]
            if "+" in uti:
                utilities2=uti.split("(")[0].split("+")[-1].split("€")[0]
                item_loader.add_value("utilities",utilities2)




        yield item_loader.load_item()

    def get_general(self, item_loader):
        keywords = {
            "square_meters": "Surface",
            "floor": "Etage",
            "utilities": "Charges",
            "room_count": "Nbre de ch",
            "bathroom_count": "Salle de bains",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(k, f".//*[@class='summary']//li[contains(.,'{v}')]/span//text()")

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
