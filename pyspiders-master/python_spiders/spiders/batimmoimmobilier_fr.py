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
from  geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'batimmoimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Batimmoimmobilier_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "http://www.batimmo-immobilier.fr/fr/location/1/"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//section[@class='properties']/article"):
            follow_url = response.urljoin(item.xpath("./div[contains(@class,'overlay-container')]/a/@href").get())
            prop_type = item.xpath(".//h2/text()").get()
            if "location" in follow_url:
                property_type = ""
                if "appartement" in prop_type.lower():
                    property_type = "apartment"
                elif "maison" in prop_type.lower():
                    property_type = "house"
                elif "studio" in prop_type.lower():
                    property_type = "apartment"
                elif "duplex" in prop_type.lower():
                    property_type = "apartment"
                elif "villa" in prop_type.lower():
                    property_type = "house"
                elif "immeuble" in prop_type.lower():
                    property_type = "house"
                if property_type != "":
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Batimmoimmobilier_PySpider_"+ self.country + "_" + self.locale)
        title = response.xpath("//h1[@class='font-lg']//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        rent="".join(response.xpath("//span[@itemprop='price']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        
        square_meters=response.xpath("//ul/li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
        
        room_count=response.xpath("//ul/li[contains(.,'Pièce(s) : ')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        latitude_longitude = response.xpath("//script[contains(.,'center')]/text()").get()
        if latitude_longitude:
            longitude = latitude_longitude.split('center: [')[1].split(',')[0]
            latitude = latitude_longitude.split('center: [')[1].split(',')[1].split(']')[0]
            geolocator = Nominatim(user_agent=response.url)
            try:
                location = geolocator.reverse(latitude + ', ' + longitude, timeout=None)
                if location.address:
                    address = location.address
                    if location.raw['address']['postcode']:
                        zipcode = location.raw['address']['postcode']
            except:
                address = None
                zipcode = None
            if address:
                item_loader.add_value("address", address)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("longitude", longitude.strip())
                item_loader.add_value("latitude", latitude.strip())
            
        external_id=response.xpath("//p[contains(.,'Ref:')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[1].strip())

        desc="".join(response.xpath("//p[contains(@itemprop,'description')]//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//ul[contains(@class,'clearing-feature')]/li//a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_xpath("landlord_name","//li/p[contains(@class,'margbot')]/text()")
        item_loader.add_xpath("landlord_phone","//li[contains(@class,'phonenumber')]/div/text()")
            
        utilties=response.xpath("//ul/li[contains(.,'charges')]/span/text()").get()
        if utilties:
            item_loader.add_value("utilities", utilties.split('€')[0].strip())
        
        deposit=response.xpath("//ul/li[contains(.,'garantie')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].strip())
        
        elevator=response.xpath("//ul/li[contains(.,'Ascenseur')]/span/text()[not(contains(.,'Non'))]").get()
        if elevator:
            item_loader.add_value("elevator",True)
        
        energy_label=response.xpath("//li[contains(@class,'note-result')]/span[1]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
            
        parking=response.xpath("//ul/li[contains(.,'Parking')]/span/text()[not(contains(.,'Non'))]").get()
        if parking :
            item_loader.add_value("parking",True)
            
        yield item_loader.load_item()