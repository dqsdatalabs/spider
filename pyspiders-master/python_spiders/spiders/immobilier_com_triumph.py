# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
import math

 
class MySpider(Spider):
    name = 'immobilier_com_triumph'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Triumphimmobilier_PySpider_france_fr"
    
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.triumph-immobilier.com/location/appartement?prod.prod_type=appt",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.triumph-immobilier.com/location/maison?prod.prod_type=house",
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='_gozzbg']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})


    # 2. SCRAPING level 2
    def populate_item(self, response):

        loue = response.xpath("//div[@class='_7y0hz3']/div/text()").extract_first()
        if loue:
            return
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Triumphimmobilier_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        rent = "".join(response.xpath("//p[contains(.,'€')]//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))        
        
        address = response.xpath("//div/span[contains(.,'Localisation')]/following-sibling::span//text()").extract_first()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())

        zipcode = response.xpath("//head/title/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split("(")[-1].split(")")[0].strip())
        
        square_meters = response.xpath("//div/span[contains(.,'Surface')]/following-sibling::span//text()").extract_first()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
            item_loader.add_value("square_meters", square_meters)
        
        
        room_count = response.xpath("//span[contains(.,'Pièces')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div/span[contains(.,'Salle')]/following-sibling::span//text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
         
        item_loader.add_xpath("title","//h1/text()")
        item_loader.add_value("external_link", response.url)

        external_id = response.xpath("//div/span[contains(.,'Référence')]/following-sibling::span//text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id)  
        
        desc = "".join(response.xpath("//div[@data-type='colExternal']//span[contains(@class,'_1qr345i')]//text()").extract())
        if desc:
            desc = desc.replace("\n","").replace("\t","").strip()
            item_loader.add_value("description", desc)
        
        images=[x for x in response.xpath("//div[contains(@class,'image')]//img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        deposit = response.xpath("//div/span[contains(.,'Dépôt de garantie')]//text()").get()
        if deposit:
            deposit = deposit.split("Dépôt de garantie")[-1].split("€")[0].strip()
            item_loader.add_value("deposit", int(float(deposit)))

        utilities = response.xpath("//div/span[contains(.,'Provision sur charges')]//text()").get()
        if utilities:
            utilities = utilities.split("Provision sur charges")[1].split("€")[0].strip()
            item_loader.add_value("utilities", int(float(utilities)))
        
        floor = response.xpath("//div/span[.='Étage']/following-sibling::span//text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        terrace=response.xpath("//div/span[contains(.,'Terrasse') or contains(.,'terrasse')]/following-sibling::span//text()").extract()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//div/span[contains(.,'Ascenseur')]/following-sibling::span//text()").extract_first()
        if elevator:
            if 'Non' in elevator:
                item_loader.add_value("elevator", False)
            elif 'Oui' in elevator:
                item_loader.add_value("elevator", True)
        
        swimming_pool=response.xpath("//div/span[contains(.,'Piscine')]/following-sibling::span//text()").extract_first()
        if swimming_pool:
            if 'Non' in swimming_pool:
                item_loader.add_value("swimming_pool", False)
            elif 'Oui' in swimming_pool:
                item_loader.add_value("swimming_pool", True)
            
        furnished=response.xpath("//div/span[contains(.,'Ameublement')]/following-sibling::span//text()").extract_first()
        if furnished:
            if 'Non' in furnished:
                item_loader.add_value("furnished", False)
            elif 'Oui' in furnished:
                item_loader.add_value("furnished", True)
            elif "Entièrement" in furnished:
                item_loader.add_value("furnished", True)
                
        balcony=response.xpath("//div/span[contains(.,'Balcon')]/following-sibling::span//text()").extract()
        if balcony:
            item_loader.add_value("balcony", True)
        
        # energy_label=response.xpath("//img[contains(@src,'conso')]/@src").extract_first()
        # if energy_label:
        #     energy_label=energy_label.split(":")[0]
        #     item_loader.add_value("energy_label", energy_label.strip())
        
        landlord_name="".join(response.xpath("//div[@class='_q2jpep']/span/text()").extract())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        
        landlord_phone = response.xpath("//span[div[span[contains(@title,'fas-phone')]]]/text()").extract_first()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        
        latitude_longitude=response.xpath("//a/@href[contains(.,'maps') and contains(.,'@')]").get()
        if latitude_longitude:
            lat=latitude_longitude.split('/@')[1].split(',')[0]
            lng=latitude_longitude.split('/@')[1].split(',')[1].split(",")[0]
            if lat or lng:
                item_loader.add_value("latitude",lat)
                item_loader.add_value("longitude", lng)
        
        
        yield item_loader.load_item()