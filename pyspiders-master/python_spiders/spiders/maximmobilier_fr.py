# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
 
class MySpider(Spider):
    name = 'maximmobilier_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "Maximmobilier_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.maximmobilier.fr/liste-LOCATION-MAISON-0.html",
                ],
                "property_type": "house"
            },
	        {
                "url": [
                    "https://www.maximmobilier.fr/liste-LOCATION-APPARTEMENT-F2.html",
                    "https://www.maximmobilier.fr/liste-LOCATION-APPARTEMENT-F3.html",
                    "https://www.maximmobilier.fr/liste-LOCATION-APPARTEMENT-F4.html"
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://www.maximmobilier.fr/liste-LOCATION-APPARTEMENT-F1.html",
                ],
                "property_type": "studio"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='a-black-underline']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//div[@class='p-2']/a[i[contains(@class,'right')]]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
            if "louer" in title.lower():
                address = title.lower().split("louer")[1].strip().capitalize()
                item_loader.add_value("address", address)
        item_loader.add_value("city","Ajaccio")
            
        rent = response.xpath("//span[contains(.,'mois')]/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//small[contains(.,'habitable')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
        room_count=response.xpath("//span[contains(.,'chambre')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0])
        square_meters=response.xpath("//small[.='Surface Habitable (m²)']/parent::div/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0])
    
        desc = "".join(response.xpath("//div[contains(@class,'container m-auto')]//p//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        terrace = response.xpath("//small[contains(.,'terrasse')]/following-sibling::span/text() | //li[contains(.,'terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
            
        elevator = response.xpath("//li[contains(.,'ascenseur') or contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//li[contains(.,'balcon') or contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'garage') or contains(.,'Garage')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        energy_label = response.xpath("//div[h3[contains(.,'DPE')]]//span[contains(@class,'circle')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        import dateparser
        available_date = response.xpath("//p/strong[contains(.,'DISPONIBLE')]/text()").get()
        if available_date and "immediatemen" not in available_date.replace("é","e").lower():
            available_date = available_date.lower().split(":")[1].replace("le","").strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//p/strong[contains(.,'GARANTIE') or contains(.,'garantie')]/text()").get()
        if deposit and ":" in deposit:
            deposit = deposit.split(":")[1].strip().split(" ")[0].replace("€","")
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//p/strong[contains(.,'ORDURES') or contains(.,'ordures')]/text()").get()
        if utilities and ":" in utilities:
            utilities = utilities.split(":")[1].strip().split(" ")[0].replace("€","")
            item_loader.add_value("utilities", utilities)
        
        latitude_longitude = response.xpath("//script[contains(.,'Longitude')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('Latitude')[1].split(';')[0].replace('=', '').strip()
            longitude = latitude_longitude.split('Longitude ')[1].split(';')[0].replace('=', '').strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        images = [x for x in response.xpath("//div[@class='swiper-wrapper']//@src").getall()]
        item_loader.add_value("images", images)
        
        item_loader.add_xpath("landlord_name", "//p[@class='h6 m-0 text-white font-bold']/text()")
        item_loader.add_value("landlord_phone", "04 95 21 01 02")
        item_loader.add_xpath("landlord_email","//a[contains(@href,'mailto')]/text()")
        
        yield item_loader.load_item()