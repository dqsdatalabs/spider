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
    name = 'delphine_teillaud_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": ["https://www.delphine-teillaud.com/location-immobiliere/appartement-studio-grenombe-isere-38"], "property_type": "studio"},
	        {"url": ["https://www.delphine-teillaud.com/location-immobiliere/location-meuble"], "property_type": "apartment"},
	        {"url": ["https://www.delphine-teillaud.com/location-immobiliere/appartement"], "property_type": "apartment"},
	        {"url": ["https://www.delphine-teillaud.com/location-immobiliere/appartement-t1-grenoble-isere-38"], "property_type": "apartment"},
	        {"url": ["https://www.delphine-teillaud.com/location-immobiliere/appartement-t2-grenoble-isere-38"], "property_type": "apartment"},
	        {"url": ["https://www.delphine-teillaud.com/location-immobiliere/appartement-t3-grenoble-isere-38"], "property_type": "apartment"},
	        {"url": ["https://www.delphine-teillaud.com/location-immobiliere/appartement-t4-grenoble-isere-38"], "property_type": "apartment"},
	        {"url": ["https://www.delphine-teillaud.com/location-immobiliere/appartement-t5-et-plus"], "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='adt_card_bien location-immobiliere']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_page:
            url = response.urljoin(next_page)
            yield Request(url, callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Delphine_Teillaud_PySpider_france")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = "".join(response.xpath("//title/text()").getall())
        if title:
            item_loader.add_value("title", title.strip())

        city = response.xpath("//section//h1/span/text()").get()
        if city: item_loader.add_value("city", city.split('\u00e0')[-1].split('ref')[0].strip())
    
        if response.xpath("//section//h2/text()[contains(.,'Non Meublé')]").get(): item_loader.add_value("furnished", False)
        elif response.xpath("//section//h2/text()[contains(.,'Meublé')]").get(): item_loader.add_value("furnished", True)

        utilities = response.xpath("//div[@class='field--label' and contains(.,'Honoraires')]/following-sibling::div/text()").get()
        if utilities: item_loader.add_value("utilities", "".join(filter(str.isnumeric, utilities.split('.')[0])))
        
        address = response.xpath("//h3[@class='adresse']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        room_count = response.xpath("//div[contains(@class,'details')]/ul/li[contains(.,'Chambres')]/span/text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[@class='field--label'][contains(.,'Pièce')]/following-sibling::div/text()[.!='0']").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(" ")[0])
            
        square_meters = response.xpath("//div[@class='left col-span-2']/p[@class='tag' ]/text()[contains(.,'m²')]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].strip())
        
        bathroom_count = response.xpath("//div[contains(@class,'details')]/ul/li[contains(.,'Salles d')]/span/text()[.!='0']").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = "".join(response.xpath("//div[@class='left col-span-2']/p[@class='prix']/text()").getall())
        if rent:
            item_loader.add_value("rent", rent.split(" ")[0].replace(" ",""))
            item_loader.add_value("currency", "EUR")
            
        energy_label = response.xpath("//img[contains(@src,'dpe-')]/@src").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("dpe-")[1].split(".")[0].upper())
        
        desc = " ".join(response.xpath("//div[@class='presentation shown']/p/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@class='slider js-slider']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
           
        external_id = response.xpath("//div[@class='left col-span-2']/p[@class='reference' ]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(" ")[-1])
        
        # import dateparser
        # available_date = response.xpath("//div[@class='field--label'][contains(.,'Disponibilit')]/following-sibling::div//text()").get()
        # if available_date:
        #     available_date = available_date.split(" ")[1]
        #     date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
        #     date2 = date_parsed.strftime("%Y-%m-%d")
        #     item_loader.add_value("available_date", date2)
        
        latitude = response.xpath("//div/@data-lat").get()
        longitude = response.xpath("//div/@data-lng").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        deposit = response.xpath("//div[@class='field--label'][contains(.,'de garantie')]/following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip())
        
        utilities = response.xpath("//div[contains(@class,'details')]/ul/li[contains(.,'Honoraires')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())

        parking = response.xpath("//div[contains(@class,'details')]/ul/li[contains(.,'Garage') or contains(.,'Parking')]/span/text()[not(contains(.,'0'))]").get()
        if parking :
            item_loader.add_value("parking", True)

        terrace = response.xpath("//div[contains(@class,'details')]/ul/li[contains(.,'Terrasse')]/text()").get()
        if parking :
            item_loader.add_value("terrace", True)

        elevator = "".join(response.xpath("//div[contains(@class,'details')]/ul/li[contains(.,'Ascenseur')]/span/text()").getall())
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)

        balcony = "".join(response.xpath("//div[contains(@class,'details')]/ul/li[contains(.,'Balcon')]/span/text()").getall())
        if balcony:
            if "non" in elevator.lower():
                item_loader.add_value("balcony", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("balcony", True)
        
        item_loader.add_xpath("landlord_name","//div[@class='infos-pro']/h3[1]/text()")
        item_loader.add_xpath("landlord_phone","//div[@class='infos-pro']/a/text()")
        
        yield item_loader.load_item()