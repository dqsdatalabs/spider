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

class MySpider(Spider):
    name = 'christinearnoux_immobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.christinearnoux-immobilier.com/annonces?id_polygon=&localisation_etendu=0&visite_virtuelle=&categorie=location&type_bien=appartement&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.christinearnoux-immobilier.com/annonces?id_polygon=&localisation_etendu=0&visite_virtuelle=&categorie=location&type_bien=maison&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//p[@class='lien-detail']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        next_page = response.xpath("//div[@class='pagelinks-next']/a/@href").get()
        if next_page:
            p_url = response.urljoin(next_page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Christinearnoux_Immobilier_PySpider_france")
        
        external_id = response.xpath("//strong[contains(.,'Réf')]//parent::li/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)    
        
        title = " ".join(response.xpath("//div[contains(@class,'desc')]//h2//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//strong[contains(.,'Ville')]//parent::li/text()").get()
        if address:
            item_loader.add_value("address", address)

        city = response.xpath("//strong[contains(.,'Ville')]//parent::li/text()").get()
        if city:
            item_loader.add_value("city", city)

        zipcode = response.xpath("//strong[contains(.,'Code')]//parent::li/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//strong[contains(.,'Surface habitable')]//parent::li/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].split(",")[0].strip()
            item_loader.add_value("square_meters", square_meters)

        rent = response.xpath("//p[contains(@class,'prix')]//span[@itemprop='price']//text()").get()
        if rent:
            rent = rent.replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        utilities = "".join(response.xpath("//strong[contains(.,'Charges')]//parent::li/text()").getall())
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@class,'desc')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//strong[contains(.,'chambres')]//parent::li/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)          
        else:
            room_count = response.xpath("//strong[contains(.,'pièce')]//parent::li/text()").get()
            if room_count:
                room_count = room_count.strip()
                item_loader.add_value("room_count", room_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'photoslider')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        terrace = response.xpath("//strong[contains(.,'Terrasse')]//parent::li/text()[contains(.,'oui')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        elevator = response.xpath("//strong[contains(.,'Ascenseur')]//parent::li/text()[contains(.,'oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//strong[contains(.,'Etage')]//parent::li/text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        latitude_longitude = response.xpath("//img[contains(@class,'map-gen')]//@src").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('|')[1].split(',')[0]
            longitude = latitude_longitude.split('|')[1].split(',')[1].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Christine Arnoux Immobilier")
        item_loader.add_value("landlord_phone", "01 44 54 01 40")
        
        yield item_loader.load_item()