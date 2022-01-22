# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser
class MySpider(Spider):
    name = 'chromeimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Chromeimmobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.chromeimmobilier.com/annonce/location-appartement.asp",
                ],
                "property_type": "apartment"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,meta={'property_type': url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='btn btn-sucess animated-hover']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
           

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//h2[@class='pt-xs']/text()").get()
        if property_type:
            item_loader.add_value("property_type", get_p_type_string(property_type))

        square_meters=response.xpath("//li[contains(.,'Surf. totale')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        room_count=response.xpath("//span[.='Nombre de pièces']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[.='Nb de salle de bains']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        external_id=response.xpath("//b[contains(.,'Référence')]/span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        description=response.xpath("//h4[.='Description du bien']/following-sibling::text()").getall()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//ul[@id='image-gallery']//li//@data-thumb").getall()]
        if images:
            item_loader.add_value("images",images)
        rent=response.xpath("//div[@class='prixbig pt-md pb-lg']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].split(":")[1].strip())
        item_loader.add_value("currency","EUR")

        item_loader.add_value("landlord_name","Chrome Immobilier")


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None