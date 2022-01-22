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
    name = 'birchills_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="Birchills_Co_PySpider_united_kingdom"
    start_urls = ['https://birchills.co.uk/advanced-search-2?filter_search_action%5B%5D=lettings&adv6_search_tab=lettings&term_id=41&term_counter=1&filter_search_type%5B%5D=&advanced_area=&min-bedrooms=&property_status=&price_low_41=500&price_max_41=15000&submit=Search+Properties']  # LEVEL 1
    

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[@class='property_listing property_unit_type4  ']/@data-link").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://birchills.co.uk/advanced-search-2/page/{page}?filter_search_action%5B0%5D=lettings&adv6_search_tab=lettings&term_id=41&term_counter=1&filter_search_type%5B0%5D=flat-apartment&advanced_area&min-bedrooms&property_status&price_low_41=500&price_max_41=15000&submit=Search%20Properties"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        external_id=response.xpath("//link[@rel='shortlink']/@href").get()
        item_loader.add_value("external_id", external_id.split("p=")[-1])

        
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//ol[@class='breadcrumb']//li[@class='active']/text()").get()
        if address:
            zipcode = address.split(",")[-1].strip().split(" ")[-1]
            city = address.split(",")[-1].strip().split(" ")[0]
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("city", city.strip())

        rent = response.xpath("//strong[contains(.,'Price')]/following-sibling::text()").get()
        if rent:
            rent = rent.strip().replace(",","").replace("£","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//strong[contains(.,'Bedrooms')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//strong[contains(.,'Bathrooms')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        available_date=response.xpath("//strong[contains(.,'Available from')]/following-sibling::text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)
        

        # furnished = response.xpath("//li//text()[contains(.,'Furnished')][not(contains(.,'Unfurnished'))]").get()
        # if furnished:
        #     item_loader.add_value("furnished", True)
        
        # balcony = response.xpath("//li//text()[contains(.,'Balcony') or contains(.,'BALCONY')]").get()
        # if balcony:
        #     item_loader.add_value("balcony", True)
        features=response.xpath("//div[@class='listing_detail col-md-4']//text()").getall()
        if features:
            for i in features:
                if "lift" in i.lower():
                    item_loader.add_value("elevator",True)
                if "parking" in i.lower():
                    item_loader.add_value("parking",True)
                
        description = " ".join(response.xpath("//div[@class='elementor-widget-container']//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
        desc =item_loader.get_output_value("description")
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        
        images = [x for x in response.xpath("//div[@class='gallery_wrapper property_header_gallery_wrapper']//div//@style").getall()]
        if images:
            img=[]
            for i in images:
                i=i.split(":url(")[-1].split(")")[0]
                img.append(i)
            item_loader.add_value("images", img)
        
        item_loader.add_value("landlord_name", "Birchills Estate Agents")
        item_loader.add_value("landlord_phone", "0208 530 7141")
        item_loader.add_value("landlord_email", "lettings@birchills.co.uk")
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None