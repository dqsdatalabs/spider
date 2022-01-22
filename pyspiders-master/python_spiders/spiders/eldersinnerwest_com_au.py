# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'eldersinnerwest_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.eldersinnerwest.com.au/rentals/search-rentals/page/1/?list=lease&type=&suburb=Suburb&property_type=Apartment&price_min=&price_max=&bedrooms=&bathrooms=&carspaces=",
                    "http://www.eldersinnerwest.com.au/rentals/search-rentals/page/1/?list=lease&type=&suburb=Suburb&property_type=Unit&price_min=&price_max=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.eldersinnerwest.com.au/rentals/search-rentals/page/1/?list=lease&type=&suburb=Suburb&property_type=House&price_min=&price_max=&bedrooms=&bathrooms=&carspaces=",
                    "http://www.eldersinnerwest.com.au/rentals/search-rentals/page/1/?list=lease&type=&suburb=Suburb&property_type=Townhouse&price_min=&price_max=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://www.eldersinnerwest.com.au/rentals/search-rentals/page/1/?list=lease&type=&suburb=Suburb&property_type=Studio&price_min=&price_max=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='listing column']/div/figure/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen: 
            base_url = f"http://www.eldersinnerwest.com.au/rentals/search-rentals/page/{page}/?"
            yield Request(base_url + response.url.split('?')[1], callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Eldersinnerwest_Com_PySpider_australia")

        ext_id = response.url.strip("/").split("/")[-1].strip()
        if ext_id:
            item_loader.add_value("external_id", ext_id)
    
        item_loader.add_xpath("title","//title/text()")
    
        address =", ".join(response.xpath("//tr[td[.='Street Address ' or .='Suburb ']]/td[2]/text()").getall())
        if address:
            item_loader.add_value("address", address.replace("\t","").strip())
        
        city = response.xpath("//tr[td[.='Suburb ']]/td[2]/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
            
        item_loader.add_xpath("zipcode", "//tr[td[.='Postcode ']]/td[2]/text()")

        room_count = response.xpath("//tr[td[.='Bedrooms ']]/td[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        elif "studio" in response.meta.get('property_type'):
            item_loader.add_value("room_count", "1")

        item_loader.add_xpath("bathroom_count", "//tr[td[.='Bathrooms ']]/td[2]/text()")

        deposit = response.xpath("//tr[td[.='Bond ']]/td[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",",""))
        square_meters = response.xpath("//tr[td[.='Building/Floor Area ']]/td[2]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])
        rent = response.xpath("//tr[td[.='Price ']]/td[2]/text()[contains(.,'$')]").get()
        if rent:
            rent = rent.lower()
            if "pw" in rent or "week" in rent:
                rent = rent.split('$')[-1].split('p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
            elif "range" in rent:
                rent = rent.split("$")[-1].strip()
                item_loader.add_value("rent", int(float(rent))*4)
            else:       
                rent = rent.split('$')[-1].split('p')[0].strip().replace(',', '').replace('\xa0', '').replace("from","")
                item_loader.add_value("rent", str(int(float(rent))))
        item_loader.add_value("currency", 'AUD')

        available_date = response.xpath("//tr[td[.='Available At ']]/td[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        description = " ".join(response.xpath("//div[@id='property_description']/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x.split("image:url(")[1].split(");")[0].strip() for x in response.xpath("//div[@id='gallery']//figure/div/@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//li/a[@class='floorplan']/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        parking = response.xpath("//tr[td[.='Garage ' or .='Carspaces ']]/td[2]/text() | //tr/td[contains(.,'Carport')]/following-sibling::td[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
            
        latlng = response.xpath("//meta[@name='geo.position']/@content").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split(";")[0].strip())
            item_loader.add_value("longitude", latlng.split(";")[1].strip())
        landlord_phone = response.xpath("//div[@class='agent_contact_info']/p[@class='agent_phone']/span/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        else:
            item_loader.add_value("landlord_phone", "//div[@class='agent_contact_info']/p[@class='agent_mobile']/span/text()")

        landlord_name = response.xpath("//div[@class='agent_contact_info']/h4/a/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
 
        yield item_loader.load_item()