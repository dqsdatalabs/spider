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
    name = 'livinglondon_net'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.living-london.net/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Apartment", "property_type": "apartment"},
	        {"url": "https://www.living-london.net/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Flat", "property_type": "apartment"},
            {"url": "https://www.living-london.net/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Mid+Terraced+House", "property_type": "house"},
            {"url": "https://www.living-london.net/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Semi-Detached+House", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@id='search-results']/div[@class='row']//h2/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        if page == 1 or seen:
            base_url = response.meta.get("base_url", response.url.replace("/search/", "/search/page_count"))
            url = base_url.replace("/page_count",f"/{page}.html")
            yield Request(url, callback=self.parse, meta={"page": page+1, "base_url":base_url,"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//h1/text()")

        item_loader.add_value("external_source", "Living_London_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)

        address = "".join(response.xpath("//h1/text()").extract())
        if address:
            item_loader.add_value("address", address)

            for i in england_city_list:
                if i.lower() in address.lower():
                    item_loader.add_value("city", i)
                    break

        rent = response.xpath("//h2[@class='h3']/text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))

        room = response.xpath("//div[@class='room-icons']/span/img[@alt='bedrooms']/following-sibling::strong/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room)
        else:
            item_loader.add_xpath("room_count", "//div[@class='room-icons']/span/img[@alt='receptions']/following-sibling::strong/text()")

        square_meters = response.xpath("//div[@class='hidden-xs hidden-sm']/p[contains(.,'Sq') or contains(.,'sq')]/text()").get()
        if square_meters:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",square_meters.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)

        bathroom_count = response.xpath("//div[@class='room-icons']/span/img[@alt='bathrooms']/following-sibling::strong/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        desc = "".join(response.xpath("//div[@class='hidden-xs hidden-sm']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [
            response.urljoin(x)
            for x in response.xpath("//div[contains(@class,'property-images')]/div//img/@src").extract()
        ]
        item_loader.add_value("images", images)

        floor_plan_images = [
            response.urljoin(x)
            for x in response.xpath("//div[@id='property-floorplans']/img/@src").extract()
        ]
        item_loader.add_value("floor_plan_images", floor_plan_images)

        latlong = " ".join(response.xpath("//script[contains(.,'googlemap')]/text()").extract())
        if latlong:
            lat = " ".join(response.xpath("substring-before(substring-after(//script[contains(.,'googlemap')]/text(),'q='),'%')").extract())
            lng = " ".join(response.xpath("substring-before(substring-after(//script[contains(.,'googlemap')]/text(),'2C'),')')").extract())
            item_loader.add_value("latitude",lat.strip())
            item_loader.add_value("longitude",lng.strip().replace("\"","") )

        furnished = " ".join(response.xpath("//ul[@class='tick']/li[contains(.,'furnished') or contains(.,'Furnished')]/text()").extract())
        if furnished:
            item_loader.add_value("furnished", True) 

        balcony = " ".join(response.xpath("//ul[@class='tick']/li[contains(.,'balcony') or contains(.,'Balcony')]/text()").extract())
        if balcony:
            item_loader.add_value("balcony", True) 

        swimming_pool = " ".join(response.xpath("//ul[@class='tick']/li[contains(.,'pool') or contains(.,'Pool')]/text()").extract())
        if swimming_pool:
            item_loader.add_value("swimming_pool", True) 


        parking = " ".join(response.xpath("//ul[@class='tick']/li[contains(.,'parking') or contains(.,'Parking')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True) 


        terrace = " ".join(response.xpath("//ul[@class='tick']/li[contains(.,'terrace') or contains(.,'Terrace')]/text()").extract())
        if terrace:
            item_loader.add_value("terrace", True) 

        item_loader.add_value("landlord_name", "Living in London")
        item_loader.add_value("landlord_phone", "020 7231 0002")
        item_loader.add_value("landlord_email", "welcome@living-london.net")


        yield item_loader.load_item()
    
england_city_list = ['Aberdeen',
 'Armagh',
 'Bangor',
 'Bath',
 'Belfast',
 'Birmingham',
 'Bradford',
 'Brighton & Hove',
 'Bristol',
 'Cambridge',
 'Canterbury',
 'Cardiff',
 'Carlisle',
 'Chelmsford',
 'Chester',
 'Chichester',
 'Coventry',
 'Derby',
 'Derry',
 'Dundee',
 'Durham',
 'Edinburgh',
 'Ely',
 'Exeter',
 'Glasgow',
 'Gloucester',
 'Hereford',
 'Inverness',
 'Kingston upon Hull',
 'Lancaster',
 'Leeds',
 'Leicester',
 'Lichfield',
 'Lincoln',
 'Lisburn',
 'Liverpool',
 'London',
 'Manchester',
 'Newcastle upon Tyne',
 'Newport',
 'Newry',
 'Norwich',
 'Nottingham',
 'Oxford',
 'Perth',
 'Peterborough',
 'Plymouth',
 'Portsmouth',
 'Preston',
 'Ripon',
 'St Albans',
 'St Asaph',
 'St Davids',
 'Salford',
 'Salisbury',
 'Sheffield',
 'Southampton',
 'Stirling',
 'Stoke-on-Trent',
 'Sunderland',
 'Swansea',
 'Truro',
 'Wakefield',
 'Wells',
 'Westminster',
 'Winchester',
 'Wolverhampton',
 'Worcester',
 'York']