# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
from html.parser import HTMLParser
import re
class MySpider(Spider):
    name = 'cotouestimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.cotouest-immobilier.com/location_resultat.html"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'bg-container-liste')]//article"):
            follow_url = response.urljoin(item.xpath(".//h3/a/@href").get())
            prop_type = item.xpath(".//a[@class='t15']/text()").get()
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
            if property_type != "":
                yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = "".join(response.xpath("//div[contains(@class,'detailsoffre')][1]//text()").extract())
        title2 = title.strip().replace('\r', '').replace('\n', '').strip()
        item_loader.add_value("title", re.sub("\s{2,}", " ", title2))
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Cotouestimmobilier_PySpider_"+ self.country + "_" + self.locale)

        latitude = response.xpath("//div[@id='collapse1']/@data-latgps").get()
        longitude = response.xpath("//div[@id='collapse1']/@data-longgps").get()
        if latitude and longitude:
            latitude = latitude.strip()
            longitude = longitude.strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

        square_meters = response.xpath("normalize-space(//span[@itemprop='category']/following-sibling::span/text())").get()
        if square_meters:
            square_meters = square_meters.split(" ")
            for i in range(len(square_meters)):
                if "m²" in square_meters[i].lower():
                    item_loader.add_value("square_meters", square_meters[i-1])
                    break
            # if len(square_meters.split('|')) >= 2:
            #     square_meters = str(int(float(square_meters.split('|')[0].split('pièce')[1].strip('s').split('m')[0].strip().replace(',', '.'))))
            #     item_loader.add_value("square_meters", square_meters)
        address = response.xpath("normalize-space(//span[@itemprop='category']/following-sibling::span/text())").get()
        if address:
            address = address.split("|")[-1].strip()
            item_loader.add_value("address", address) 
            item_loader.add_value("city", address) 
        zipcode=response.url
        if zipcode:
            zipcode=re.findall("\d{4,6}",zipcode)
            if zipcode:
                item_loader.add_value("zipcode",zipcode)

        room_count = response.xpath("normalize-space(//span[@itemprop='category']/following-sibling::span/text())").get()
        if room_count:
            room_count = room_count.split(" ")
            for i in range(len(room_count)):
                if "pièce" in room_count[i].lower():
                    item_loader.add_value("room_count", room_count[i-1])
                    break
            # if len(room_count.split('|')) >= 2:
            #     room_count = room_count.split('|')[1].strip().split(' ')[0]
            #     item_loader.add_value("room_count", room_count)

        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace('\xa0', '').replace(' ', '')
            if rent.isnumeric():
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'EUR')

        external_id = response.url.split('_')[-1].split('.')[0].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@class='desc_offre']//text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        # city = response.xpath("//span[@itemprop='category']/following-sibling::span/text()").get()
        # if city:
        #     if len(city.split('|')) > 2:
        #         city = city.split('|')[2].strip()
        #         item_loader.add_value("city", city)

        images = [x for x in response.xpath("//div[@id='gallery']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//br[contains(following-sibling::text(),'Dépôt de garantie')]/following-sibling::text()[1]").get()
        if deposit:
            deposit = deposit.split('Dépôt de garantie')[1].split('€')[0].strip().replace(' ', '')
            item_loader.add_value("deposit", deposit)
        utilities = response.xpath("//br[contains(following-sibling::text(),'Dont ') and contains(following-sibling::text(),'charges ')]/following-sibling::text()[1]").get()
        if utilities:
            utilities = utilities.split('(')[0].split('€')[0].strip().replace(' ', '')
            item_loader.add_value("utilities", utilities)
        landlord_name = response.xpath("//p[@class='coordonnees-agence hidden-xs'][1]/a[1]/text()").get()
        if landlord_name:
            landlord_name = landlord_name.split(':')[0].strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//p[@class='coordonnees-agence hidden-xs'][1]/a[1]/following-sibling::text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split('Tél.')[1].split('-')[0].strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//p[@class='coordonnees-agence hidden-xs'][1]/a[2]/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data