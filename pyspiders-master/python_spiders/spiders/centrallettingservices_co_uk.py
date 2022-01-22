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
    name = 'centrallettingservices_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {"url": "http://www.centrallettingservices.co.uk/?s=apartment&post_type=properties", "property_type": "apartment"},
            {"url": "http://www.centrallettingservices.co.uk/?s=house&post_type=properties", "property_type": "house"},
        ]  # LEVEL 1
    
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//article//div//following-sibling::img/parent::a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            if response.meta.get('property_type')=='apartment':
                url = f"http://www.centrallettingservices.co.uk/page/{page}/?s=apartment&post_type=properties#038;post_type=properties"
                yield Request(url, callback=self.parse, meta={"page": page+1, "property_type" : response.meta.get("property_type")})

            elif response.meta.get('property_type')=='house':
                url = f"http://www.centrallettingservices.co.uk/page/{page}/?s=house&post_type=properties#038;post_type=properties"
                yield Request(url, callback=self.parse, meta={"page": page+1, "property_type" : response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # if response.xpath("//span[contains(.,'Availability')]/following-sibling::text()[contains(.,'Let Agreed')]").get(): return

        item_loader.add_value("external_source", "Centrallettingservices_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//header/h2/text()")
        item_loader.add_value("external_link", response.url)
    
        rent=response.xpath("normalize-space(//div[@class='gw-price']/h2/text())").get()         
        if "Per Day" in rent:
            price=rent.split("Per")[0].split("£")[1].strip()
            if "." in price:
                price=rent.split(".")[0]
                item_loader.add_value("rent_string", str(int(price)*30)+"£")
            else: item_loader.add_value("rent_string", str(int(price)*30)+"£")
        elif rent:
            if "." in rent:
                price=rent.split(".")[0]
                item_loader.add_value("rent_string", price)
            else: item_loader.add_value("rent_string", rent)
            
        address=response.xpath("//div[@class='gw-details']/ul/li[contains(.,'Address')]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            try:
                zipcode = address.split(",")[-1]
                city = address.split(",")[-2].strip()
                if "UK" in zipcode or "United Kingdom" in zipcode:
                    zipcode = address.split(",")[-2]
                    zipcode = zipcode.strip().split(" ")[-2]+" "+zipcode.strip().split(" ")[-1]
                    city = address.split(",")[-3].strip()
                if len(zipcode.strip().split(" "))>2:
                    zipcode = zipcode.strip().split(" ")[-2]+" "+zipcode.strip().split(" ")[-1]
                
                item_loader.add_value("zipcode", zipcode.replace("City","").strip())
                item_loader.add_value("city",city)
            except:
                pass
            
            
        room_count=response.xpath("//div[@class='gw-details']/ul/li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count=response.xpath("//div[@class='gw-details']/ul/li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        deposit=response.xpath("//div[@class='gw-details']/ul/li[contains(.,'Deposit')]/text()[not(contains(.,'TBC'))]").get()
        if deposit and "." in deposit:
            item_loader.add_value("deposit", deposit.split(".")[0])
        elif deposit:
            item_loader.add_value("deposit", deposit.split('£')[1])
        
        desc="".join(response.xpath("//div[@class='gw-description']//text()").getall())
        if desc:
            item_loader.add_value("description", desc)
        if "lift" in desc.lower():
            item_loader.add_value("elevator", True)
            
        images=[x for x in response.xpath("//div/ul[contains(@class,'mgop-elements')]/li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        furnished=response.xpath("//ul[@class='clearfix']/li[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        energy_label=response.xpath("//ul[@class='clearfix']/li[contains(.,'EPC')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("Rating")[1].strip())
        
        parking=response.xpath("//ul[@class='clearfix']/li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        latitude_longitude=response.xpath("//script[@type='text/javascript'][contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude=latitude_longitude.split('center: {lat:')[-1].split(',')[0].strip()
            longitude=latitude_longitude.split('lng: ')[-1].split('}')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_name","CENTRAL LETTING SERVICES")
        phone=response.xpath("//div[@class='gw-details']/ul/li[contains(.,'Phone')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        item_loader.add_value("landlord_email", "info@centrallettingservices.co.uk")

        if not item_loader.get_collected_values("zipcode"):
            zipcode = response.xpath("//div[@class='gw-address']/p/text()").get()
            if zipcode:
                zipcode = " ".join(zipcode.strip().split(" ")[-2:]).strip()
                if "United" not in zipcode:
                    item_loader.add_value("zipcode", zipcode)
        
        if not item_loader.get_collected_values("city"):
            city = response.xpath("//div[@class='gw-address']/p/text()").get()
            if city: 
                item_loader.add_value("city", city.split(" ")[0].strip())

        yield item_loader.load_item()