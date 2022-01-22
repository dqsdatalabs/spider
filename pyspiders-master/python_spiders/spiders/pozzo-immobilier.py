# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'pozzoimmobilier'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Pozzoimmobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.pozzo-immobilier.fr/louer/nos-offres/appartement",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.pozzo-immobilier.fr/louer/nos-offres/maison",
                ],
                "property_type" : "house"
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='d-flex']/following-sibling::a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//div[@class='pagination__items pagination__items--forward ml-10']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title","//title//text()")

        external_id=response.xpath("//span[contains(.,'réf')]/following-sibling::div/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id) 

        rent=response.xpath("//div[@class='h3 m-0 text-primary']/text()").get()
        if rent:
            rent=rent.split("€")[0].strip()
            if " " in rent:
                rent=rent.replace(" ","")
                item_loader.add_value("rent",rent)
            else:
                item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//div[@class='title h3 m-0']/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("-")[-1])
        room_count=response.xpath("//div[@class='field field--rooms']/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("\xa0")[0])
        bathroom_count=response.xpath("//div[@class='field field--bathrooms']/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("\xa0")[0])
        desc=response.xpath("//h2[.='Présentation du bien']/following-sibling::div/p/text()").get()
        if desc:
            item_loader.add_value("description",desc)
        adres=response.xpath("//div[@id='base-infos']//div[contains(.,'Ville')]/following-sibling::div/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//div[@id='base-infos']//div[contains(.,'Ville')]/following-sibling::div/text()").get()
        if city:
            item_loader.add_value("city",city.split(" ")[0])
            item_loader.add_value("zipcode",city.split("(")[-1].split(")")[0])
        latitude=response.xpath("//meta[@property='place:location:latitude']/@content").get()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//meta[@property='place:location:longitude']/@content").get()
        if longitude:
            item_loader.add_value("longitude",longitude)
        images=[response.urljoin(x) for x in response.xpath("//div[@class='swiper-wrapper']//img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        deposit=response.xpath("//div[@id='base-infos']//div[contains(.,'Dépôt de garantie')]/following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(",")[0])
        utilities=response.xpath("//div[@id='base-infos']//div[contains(.,'Provision sur charges')]/following-sibling::div/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(",")[0])
        floor=response.xpath("//div[@id='base-infos']//div[contains(.,'Etage')]/following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor",floor)

        landlord_name=response.xpath("//div[contains(@class,'d-flex align-items-center font-weight-bold')]//div/text()").getall()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)

        landlord = response.xpath("//div[@class='mt-20']//a//@href").get()
        url = f"https://www.pozzo-immobilier.fr/{landlord}"
        yield Request(
            url, 
            callback=self.landlord_item, 
            meta={'item': item_loader}
        )
            # yield Request(response.urljoin(landlord), callback=self.get_landlord_details, meta={"item_loader":response.meta.get('item_loader')})

    def landlord_item(self, response):
        item_loader = response.meta.get("item")

        landlord_phone=response.xpath("//div[contains(@class,'field--phone taxonomy-term')]//a/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone",landlord_phone)

        landlord_email=response.xpath("//div[contains(@class,'field--mail taxonomy-term')]//a/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email",landlord_email)
        yield item_loader.load_item()