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
import math

class MySpider(Spider):
    name = 'booster_immobilier_com'  
    execution_type='testing'
    country='france'
    locale='fr'
    scale_separator = '.' 

    def start_requests(self):
        start_urls = [
            {"url": "https://www.booster-immobilier.com/fr/liste.htm?page=1&ope=2#page=1&ope=2&filtre2=2",
            "prop_type": "apartment"},
            {"url": "https://www.booster-immobilier.com/fr/liste.htm?page=1&ope=2#page=1&ope=2&filtre8=8",
            "prop_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={"type":url.get('type'), "property_type":url.get('prop_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        max_page = response.xpath("//span[@class='nav-page-position']/text()").get()
        if max_page:
            max_page = int(max_page.split('/')[1].strip())

        for item in response.xpath("//div[@id='ListeAnnonce']//a[@itemprop='url']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        if int(page) <= max_page:
            url = ''
            if response.meta.get('property_type') == 'apartment':
                url = f"https://www.booster-immobilier.com/fr/liste.htm?page={page}&ope=2#page={page}&ope=2&filtre2=2"
            else:
                url = f"https://www.booster-immobilier.com/fr/liste.htm?page={page}&ope=2#page={page}&ope=2&filtre8=8"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.meta.get("property_type")
        item_loader.add_value("property_type", property_type)

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Boosterimmobilier_PySpider_"+ self.country + "_" + self.locale)
        title=response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        latitude = response.xpath("//li[@class='gg-map-marker-lat']/text()").get()
        longitude = response.xpath("//li[@class='gg-map-marker-lng']/text()").get()
        if latitude and longitude:
            latitude = latitude.strip()
            longitude = longitude.strip()

            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        address = "".join(response.xpath("//ul[@class='specs-text']/li[span[.='Ville']]/text()").extract())
        if address:
            item_loader.add_value("address", address.strip())

        zipcode = response.xpath("//span[@itemprop='postalCode']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        square_meters = response.xpath("//ul[@class='nolist']//span[contains(@class,'surface')]/following-sibling::text()").get()
        if square_meters:
            if not 'NC' in square_meters:
                square_meters = str(int(float(square_meters.split('m')[0].strip())))
                item_loader.add_value("square_meters", square_meters)

        # room_count = response.xpath("//ul[@class='nolist']/li[contains(.,'pièce')]/text()").get()
        # if room_count:
        #     if not 'NC' in room_count:
        #         room_count = room_count.split('pièce')[0].strip()
        #         item_loader.add_value("room_count", room_count)
        #     elif "pièce" in title:
        #         room=title.split("pièce")[0].strip().split(" ")[-1]
        #         item_loader.add_value("room_count", room)

        room_count = response.xpath("//ul[@class='nolist']/li[contains(.,'chambre')]/text()[not[contains(.,'NC chambre(s)')]]").get()
        if room_count:
            room_count = room_count.split('chambre')[0].strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[contains(@class,'detail-bien-specs')]//text()[contains(.,'pièce')]").get()
            if room_count:
                room_count = room_count.split("pièce")[0].strip()
                item_loader.add_value("room_count", room_count)


                
            
        rent = response.xpath("//div[@class='price-all']/br/following-sibling::text()").get()
        if rent:
            price = rent.split('€')[0].strip().replace('\xa0', '').replace(' ', '')
            item_loader.add_value("rent", math.ceil(float(price)))
            item_loader.add_value("currency", "EUR")

        external_id = response.xpath("//div[@class='ref']/span[2]/span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        description = response.xpath("//p[@class='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        city = response.xpath("//span[contains(.,'Ville')]/following-sibling::text()").get()
        if city:
            city = city.strip()
            item_loader.add_value("city", city)

        images = [x for x in response.xpath("//div[@class='large-flap-container']/div[contains(@class,'is')]/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        deposit = response.xpath("//span[contains(.,'Dépôt de garantie')]/following-sibling::span/text()").get()
        if deposit:
            deposit = deposit.strip().replace('\xa0', '').replace(' ', '').strip()
            if deposit !="0":
                item_loader.add_value("deposit", deposit)

        parking = response.xpath("//p[@class='description']/text()[contains(.,'parking') or contains(.,'Parking') ]").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//p[@class='accroche']/text()[contains(.,'meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        utilities = response.xpath("//li[contains(@class,'prix')]/i/span[contains(.,'Dont provisions sur charges ')]/following-sibling::span[1]/text()").get()
        if utilities:
            item_loader.add_value("utilities", int(float(utilities)))
        else:
            item_loader.add_xpath("utilities", "//span[@class='cout_charges_mens']/text()")

        energy_label = response.xpath("substring-before(substring-after(//div[@class='NrjGrad']/img/@src,'ion-'),'.')").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))
            
        landlord_phone = response.xpath("//span[@itemprop='telephone']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_name", "Booster Immobilier")
        
        status=response.xpath("//ul/li/span[contains(.,'Type')]/following-sibling::span/text()").get()
        if ("Garage" not in status) and ("Parking" not in status) and ("commercial" not in status) and ("Bureau" not in status) :
            yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label