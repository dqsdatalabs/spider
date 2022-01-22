# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
class MySpider(Spider):
    name = 'rightletleeds_co_uk'
    execution_type='testing' 
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.rightletleeds.co.uk/search.php?saletype=1&lettingType=professional",
                ],
                "property_type": ""
            },
	        {
                "url": [
                    "https://www.rightletleeds.co.uk/search.php?saletype=1&searchText=&lettingType=student&bedrooms="
                ],
                "property_type": "student_apartment"
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
        
        for item in response.xpath("//div[@class='property']"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            let_agreed = item.xpath(".//img[@alt='Let Agreed']").get()
            if let_agreed:
                continue
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        next_page_url = response.xpath("//div[@class='paging']//a[@class='navbynumbers_Next']/@href").get()   
        if next_page_url:
            url = response.urljoin(next_page_url)
            yield Request(url, callback=self.parse, meta={"property_type": response.meta.get('property_type')})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//div[@id='address']//h2/text()").getall())
        if response.meta.get('property_type'):
            item_loader.add_value("property_type", response.meta.get('property_type'))
        elif get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        
        item_loader.add_value("external_source", "Rightlet_PySpider_united_kingdom")  
        item_loader.add_xpath("title", "//div[@id='address']//h1/text()")     
        item_loader.add_value("external_id", response.url.split("&id=")[-1])  
    
        address = response.xpath("//div[@id='address']//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())  
            item_loader.add_value("zipcode", address.split(",")[-1].strip())  
            item_loader.add_value("city", address.split(",")[-2].strip())  
      

        item_loader.add_xpath("room_count", "//i[@class='fas fa-bed']/preceding-sibling::text()[1]") 
        item_loader.add_xpath("bathroom_count", "//i[@class='fas fa-bath']/preceding-sibling::text()[1]") 
        rent = response.xpath("//p[@class='price']/span/text()").get()
        if rent:
            rent=rent.replace("£","").strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency","EUR")
        description = " ".join(response.xpath("//div[@id='tab-description']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//div[@id='propertyImage']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Right Let Leeds")
        item_loader.add_value("landlord_email", "enquiries@rightletleeds.co.uk")
        item_loader.add_value("landlord_phone", "0113 274 9499")

        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)  
            else:
                item_loader.add_value("furnished", True)
        javascript = response.xpath("//script[contains(.,'google.maps.LatLng(')]/text()").extract_first()
        if javascript:        
            latitude = javascript.split("google.maps.LatLng(")[1].split(",")[0]
            longitude = javascript.split("google.maps.LatLng(")[1].split(",")[1].split(")")[0]
            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)
        balcony = response.xpath("//li[contains(.,'balcony') or contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)  
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True) 
    

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "garagebox " in p_type_string.lower():
        return None
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"    
    else:
        return None