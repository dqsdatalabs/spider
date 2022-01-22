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
    name = 'pouletimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.pouletimmobilier.fr/search?typeTransaction=Location&typeBien=appartement&nbPiecesMin=&localisation=&refProduit=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.pouletimmobilier.fr/search?typeTransaction=Location&typeBien=maison&nbPiecesMin=&localisation=&refProduit=",
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
        for item in response.xpath("//div[@class='bienBottom']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page and "javascript" not in next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        parking_type = response.xpath("//p[@itemprop='description']//text()[contains(.,'PARKING - COTY/FLAUBERT :')]").get()
        if parking_type:
            return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Pouletimmobilier_PySpider_france")          
        item_loader.add_xpath("title","//h1/text()")
        item_loader.add_xpath("external_id", "//div[div[.='Ref : ']]/div[2]/span/text()")
        item_loader.add_xpath("utilities", "//div[div[.='Charges : ']]/div[2]/span/text()")
        item_loader.add_xpath("address", "//div[div[.='Ville : ']]/div[2]/span/text()")
        room_count = response.xpath("//div[div[.='Nb. chambres : ']]/div[2]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[@class='col-sm-8']//div/text()[contains(.,'pièces')]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("pièces")[0])
        city = response.xpath("//div[div[.='Ville : ']]/div[2]/span/text()").get()
        if city:
            zipcode = city.split(" ")[-1]
            city = city.replace(zipcode,"")
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())
        square_meters = response.xpath("//div[div[.='Surface habitable : ']]/div[2]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.split("m")[0].strip()))))
     
        rent = response.xpath("//div[div[.='Prix : ']]/div[2]/span/text()").get()
        if rent:
            item_loader.add_value("rent", int(float(rent.split("€")[0].replace(" ","").strip())))
        item_loader.add_value("currency", "EUR")
        deposit = response.xpath("//span[@class='alur_depot_garantie']/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].split(":")[-1].replace(" ","").strip()
            item_loader.add_value("deposit", int(float(deposit)))

        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        parking = response.xpath("//p[@itemprop='description']//text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        images = [x for x in response.xpath("//div[contains(@class,'carouselDetail')]/div/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "Cabinet Poulet")
        item_loader.add_value("landlord_phone", "02 35 42 30 63")
        item_loader.add_value("landlord_email", "pouletimmobilier@orange.fr")
        yield item_loader.load_item()