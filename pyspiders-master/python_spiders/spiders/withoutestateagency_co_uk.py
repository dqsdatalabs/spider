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
    name = 'withoutestateagency_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.withoutestateagency.co.uk/rental-properties-uk/?address=&type_of_ad=offer_for_rent&property_type%5B%5D=Flat&price=&price_to=&search=search&search=search&sort=latest",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.withoutestateagency.co.uk/rental-properties-uk/?address=&type_of_ad=offer_for_rent&property_type%5B%5D=House&price=&price_to=&search=search&search=search&sort=latest"
                ],
                "property_type": "house"
            }
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
        
        for item in response.xpath("//div[contains(@class,'serachi-list info')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Withoutestateagency_Co_PySpider_united_kingdom")  
        item_loader.add_xpath("title", "//h1/text()")     
        item_loader.add_xpath("external_id", "//tr[td[.='Advertisement code:']]/td[2]/text()")  
        city = ""
        address = " ".join(response.xpath("//h1/span//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())  
            zipcode = address.split(",")[0].strip()
            if not zipcode.replace(" ","").isalpha() and len(zipcode.split(" "))<3 and "Flat" not in zipcode:
                item_loader.add_value("zipcode",zipcode )
            if "London" in address:
                city = "London" 
                item_loader.add_value("city",city)  
        if city == "":
            city = response.url.split("/")[-2].capitalize().replace("-"," ")
            if city and city.isalpha():
                item_loader.add_value("city",city )   
                      
        energy_label = response.xpath("//tr[td[.='Energy performance of buildings:']]/td[2]/text()[.!='Unknown']").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("-")[0].strip()) 
        room_count = response.xpath("//tr[td[.='Disposition:']]/td[2]/text()[contains(.,'Bedroom')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bedroom")[0].strip()) 
        rent = response.xpath("//div[@class='price-qty']/text()").get()
        if rent and rent.strip() != "£ to" and rent.strip() != "£ + / month":
            if "week" in rent:
                rent = rent.split("£")[-1].split("/")[0].replace(",","").strip()
                item_loader.add_value("rent", str(int(rent)*4))
            else:
                item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", "GBP")
        description = " ".join(response.xpath("//div[@class='ads-details']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//div[@id='carousel']/ul[@class='slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        landlord_phone = response.xpath("//div[@class='ad--info']/p/text()[contains(.,'Phone Number:')]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.split(":")[-1])
        else:
            landlord_phone = response.xpath("//div[@class='row']/comment()").re_first(r'"tel:(\d+)"')
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone)
        landlord_email = response.xpath("//div[@class='ad--info']/p/text()[contains(.,'Email:')]").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.split(":")[-1])
        else:
            item_loader.add_value("landlord_email", "info@withoutestateagency.co.uk")

        item_loader.add_value("landlord_name", "Without Estate Agency")
      
        terrace = response.xpath("//tr[td[.='Terace:']]/td[2]/text()").get()
        if terrace:
            if "no" in terrace.lower():
                item_loader.add_value("terrace", False)  
            elif "yes" in terrace.lower():
                item_loader.add_value("terrace", True)  
        pets_allowed = response.xpath("//tr[td[.='Pets allowed:']]/td[2]/text()").get()
        if pets_allowed:
            if "no" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", False)  
            elif "yes" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", True)  
        furnished = response.xpath("//tr[td[.='Furnished:']]/td[2]/text()").get()
        if furnished:
            if "no" in furnished.lower():
                item_loader.add_value("furnished", False)  
            else:
                item_loader.add_value("furnished", True)
        balcony = response.xpath("//tr[td[.='Balcony:']]/td[2]/text()").get()
        if balcony:
            if "no" in balcony.lower():
                item_loader.add_value("balcony", False)  
            elif "yes" in balcony.lower():
                item_loader.add_value("balcony", True)  
        parking = " ".join(response.xpath("//tr[td[.='Garage:' or .='Off street parking:']]/td[2]/text()").getall())
        if parking:
            if "yes" in parking.lower():
                item_loader.add_value("parking", True) 
            elif "no" in parking.lower():
                item_loader.add_value("parking", False) 

        yield item_loader.load_item()