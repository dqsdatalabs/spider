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
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'agence_samim_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = 'Agence_Samim_PySpider_france'
    def start_requests(self, **kwargs):


        start_urls = [
            {"url": "https://www.agence-samim.com/a-louer/appartements/1", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//article[@class='card']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, dont_filter=True, callback=self.populate_item, meta={'property_type': response.meta['property_type']})

        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Agence_Samim_PySpider_"+ self.country)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)
            
        address = response.xpath("//tr[th[.='Ville']]/th[2]/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
            
        zipcode = response.xpath("//tr[th[.='Code postal']]/th[2]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
            
        square_meters = response.xpath("//tr[th[contains(.,'Surface')]]/th[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
            item_loader.add_value("square_meters", str(math.ceil(float(square_meters.replace(",",".")))))
            
        room_count = response.xpath("//tr[th[contains(.,'chambre')]]/th[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//ul/li[contains(.,'pièces')]/span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
            
        
        bathroom_count = response.xpath("//tr[th[contains(.,'Nb de salle d')]]/th[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//h2[@class='detail-price']/text()[contains(.,'€')]").get()
        if rent:
            price = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", price)
        
        item_loader.add_value("currency", "EUR")
        
        external_id = response.xpath("//h2[@class='detail-price']/text()[4]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat :")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("lng:")[1].split("}")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        desc = "".join(response.xpath("//p[@class='description']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        if "Disponible imm\u00e9" in desc:
            available_date = datetime.now().strftime("%Y-%m-%d")
            item_loader.add_value("available_date", available_date)
        elif "Disponible le" in desc:
            available_date = desc.split("Disponible le")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [ x for x in response.xpath("//ul[@class='imageGallery notLoaded']/li/@data-thumb").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor = response.xpath("//ul/li[contains(.,'Etage')]/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        deposit = response.xpath("//tr[th[contains(.,'garantie')]]/th[2]/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", deposit)
            
        utilities = response.xpath("//tr[th[contains(.,'charge locataire')]]/th[2]/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip().replace(" ","")
            item_loader.add_value("utilities", utilities)
            
        furnished = response.xpath("//ul/li[contains(.,'Meublé')]/span/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//tr[th[contains(.,'Ascenseur')]]/th[2]/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//ul/li[contains(.,'Balcon')]/span/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            elif "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//tr[th[contains(.,'Meublé')]]/th[2]/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            elif "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
        
        item_loader.add_value("landlord_name", "SAMIM AGENCY")

        if "Contact" in desc:
            landlord_phone = desc.split("Contact")[1].strip().split(" ")[-1]
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "04 48 76 80 00")

        item_loader.add_value("landlord_email", "cevennes@samim.fr")
            
        
        yield item_loader.load_item()

