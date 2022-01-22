# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from html.parser import HTMLParser

class MySpider(Spider):
    name = 'keymex_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.keymex.fr/location/maison/",
                "property_type" : "house"
            },
            {
                "url" : "https://www.keymex.fr/location/appartement/",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='list-annonces list-loop']//article//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        
        item_loader.add_value("external_link", response.url.split("?search_id")[0])
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_source", "Keymex_PySpider_"+ self.country + "_" + self.locale)

        address = " ".join(response.xpath("//div[@class='nav-top-single']/span/a/text()").getall())
        if address:
            zipcode = address.strip().split("(")[1].split(")")[0].strip()
            city = address.strip().split("à")[1].split("(")[0].strip()
            item_loader.add_value("address", address.strip().split("à")[1].strip())
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        # latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        # if latitude_longitude:
        #     latitude = latitude_longitude.split('google.maps.LatLng(')[1].split(',')[0]
        #     longitude = latitude_longitude.split('google.maps.LatLng(')[1].split(',')[1].split(')')[0]
        #     item_loader.add_value("longitude", longitude.strip())
        #     item_loader.add_value("latitude", latitude.strip())
            

        square_meters = response.xpath("//div[@class='mb-4']/div/div/strong[contains(.,'m²')]/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m²')[0].strip())))
            item_loader.add_value("square_meters", square_meters)
        
        room_count=response.xpath("//div[@class='mb-4']/div/div/strong[contains(.,'chambres')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0].strip()) 
        else:       
            room_count=response.xpath("//div[@class='mb-4']/div/div/strong[contains(.,'pi')]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(" ")[0].strip()) 

        rent = response.xpath("//span[@class='price block mt-2 size-40 f-700 cl-primary']/span/text()").get()
        if rent:
            rent = rent.replace(" ","").replace("\xa0","")
            item_loader.add_value("rent_string",rent )
           
        # currency = 'EUR'
        # item_loader.add_value("currency", currency)         
 
        external_id = response.url.split("-")[-1].replace("/","")
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        description = " ".join(response.xpath("//section[@class='mb-4']/p/text()").getall())         
        if description:            
            item_loader.add_value("description", description)
            if "meublé" in description or "meublée" in description:
                item_loader.add_value("furnished",True)
            try:
                if "étage" in description:
                    floor = description.split("étage")[0].strip().split(" ")[-1].strip()
                    if floor:
                        item_loader.add_value("floor",floor)
                if "salle de bains" in description:
                    bathroom_count = description.split("salle de bains")[0].strip().split(" ")[-1].strip()
                    if bathroom_count.isdigit():
                        item_loader.add_value("bathroom_count",bathroom_count)
            except:
                pass
        
        if "studio" in description.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        images = [x for x in response.xpath("//figure[contains(@class,'slide')]/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        deposit = response.xpath("//span[@class='alur_location_depot']/text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip()
            item_loader.add_value("deposit",int(float(deposit.replace(" ","").replace("\xa0",""))) )

        item_loader.add_value("landlord_name", "Keymex")
        item_loader.add_value("landlord_email", "location@keymexfrancegestion.fr")
        energy = response.xpath("substring-before(substring-after(//figure[@class='dpe inline-block']/img/@src,'ges/dpe-ges/dpe-'),'.')").get()
        if energy:
            item_loader.add_value("energy_label", energy.upper())                
        yield item_loader.load_item()


       

        
        
          

        

      
     