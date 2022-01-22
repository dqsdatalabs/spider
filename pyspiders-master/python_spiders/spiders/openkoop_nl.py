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

class MySpider(Spider):
    name = 'openkoop_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.funda.nl/huur/heel-nederland/appartement/", "property_type": "apartment"},
	        
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "type":url.get('type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//ol/li[contains(@class,'search-result')]//div/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,  meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 1 or seen:
            url = f"https://www.funda.nl/huur/heel-nederland/appartement/p{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Openkoop_PySpider_" + self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//span[@class='object-header__title']/text()")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        rent="".join(response.xpath("//dl/dt[contains(.,'Huurprijs')]//following-sibling::dd[1]/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        else: return
        
        square_meters=response.xpath("//span[contains(.,'m²')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
        else: return
        
        room_count=response.xpath(
            "//div/dl/dt[contains(.,'Aantal kamers')]/following-sibling::dd[1]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(' ')[0])
        else: return

        latitude_longitude = response.xpath("//script[@type='application/json'][contains(.,'lat')]/text()").get()
        if latitude_longitude:
                latitude = latitude_longitude.split('lat":')[1].split(',')[0]
                longitude = latitude_longitude.split('lng":')[1].split(',')[0]
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
                    item_loader.add_value("longitude", longitude)
                    item_loader.add_value("latitude", latitude)
                else: return
        else: return

        desc="".join(response.xpath("//div[@class='object-description-body']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        floor=response.xpath(
            "//div/dl/dt[contains(.,'woonlagen')]/following-sibling::dd[1]//text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(' ')[0])
        
        energy_label="".join(response.xpath("//div/dl/dt[contains(.,'energie')]/following-sibling::dd[1]//text()").getall())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(' ')[0])
            
        images=[x for x in response.xpath("//div[@class='carousel-list']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit=response.xpath(
            "//dl/dt[contains(.,'Waarborgsom')]//following-sibling::dd[1]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('eenmalig')[0].split(' ')[-1])      

        yield item_loader.load_item()