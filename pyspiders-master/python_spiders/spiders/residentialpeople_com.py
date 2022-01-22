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
    name = 'residentialpeople_com'
    execution_type='testing' 
    country='united_kingdom'
    locale='en'
    external_source='Residentialpeople_PySpider_united_kingdom'
    thousand_separator = ','
    scale_separator = '.'       
    start_urls = ['https://www.residentialpeople.com/gb/property-for-rent/']  # LEVEL 1

    def start_requests(self):
        yield Request(
            url=self.start_urls[0],
            callback=self.jump,
        )

    def jump(self, response):
        for item in response.xpath("//div[@class='seo-locations']//ul/li//text()[not(contains(.,'Propert'))]").getall():
            f_url = f"https://www.residentialpeople.com/gb/property-for-rent/{item.lower().replace(' ','-')}/?country=gb&listing_type=residential&transaction_type=rent&longitude=-2.122353&latitude=57.141266&size_qualifier=square_feet&sort_by=closest_to_farthest&offset=0&limit=10&active=1&location_slug=aberdeen"
            yield Request(f_url, callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 10)
        seen = False
        for item in response.xpath("//a[contains(@class,'link--custom')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"&offset={page-10}", f"&offset={page}")
            yield Request(url, callback=self.parse, meta={"page": page+10})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        check_status = response.url
        check_status = check_status.split('.')[0].strip()
        if check_status and "https://news" in check_status:
            return
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1/text()").get()
        if title and "flat" in title.lower():
            item_loader.add_value("property_type", "apartment")
        elif get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))
        else: return
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1/text()")    
        rent = response.xpath("//div[@class='property-details-header-price__secondary']/text()").get()
        if rent:
            rent=rent.split("p")[0].split("£")[-1].replace(",","").strip()
            item_loader.add_value("rent",int(rent))  
        
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[@class='property-details-description__text']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        item_loader.add_xpath("room_count", "//span[contains(@class,'property-details__rooms')][span[@class='icon icon--bed']]//text()")
        item_loader.add_xpath("bathroom_count", "//span[contains(@class,'property-details__rooms')][span[@class='icon icon--bathtub-1']]//text()")
        parking = response.xpath("//li/span/text()[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//li/span/text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        available_date = response.xpath("//li/span/text()[contains(.,'Available ')]").get()
        if available_date and "now" not in available_date.lower():
            date_parsed = dateparser.parse(available_date.split("Available ")[-1].strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        json_data = response.xpath("//script[@type='application/json']//text()").get()
        if json_data:
            data = json.loads(json_data)["props"]["initialState"]["selectedProperty"]["data"]
            
            item_loader.add_value("external_id", data["agentReference"])
            item_loader.add_value("latitude", str(data["coordinates"]["lat"]))
            item_loader.add_value("longitude", str(data["coordinates"]["lng"]))
            item_loader.add_value("address", data["address"]["displayAddress"])
            item_loader.add_value("zipcode", data["address"]["postcode"])
            zipcodecheck=item_loader.get_output_value("zipcode")
            if not zipcodecheck:
                zipcode=response.xpath("//p[@class='property-details-header__address mb-10']/text()").get()
                item_loader.add_value("address",zipcode.split(",")[-2].split(" ")[0])
                item_loader.add_value("zipcode",zipcode.split(",")[-2].split(" ")[-2].strip())
            idcheck=item_loader.get_output_value("external_id")
            if not idcheck:
                item_loader.add_value("external_id", data["shortCode"])
   
            item_loader.add_value("city", data["address"]["town"])
            citycheck=item_loader.get_output_value("city")
            if not citycheck:
                city=data["address"]["displayAddress"]
                item_loader.add_value("city",city.split(",")[-2].strip())

            images = [x["original"] for x in data["formattedImages"]]
            if images:
                item_loader.add_value("images", images)
            item_loader.add_value("landlord_email", data["branch"]["email"])
            emailcheck=item_loader.get_output_value("landlord_email")
            if not emailcheck:
                item_loader.add_value("landlord_email", data["branch"]["rentsEmail"])




            
        landlord_phone = response.xpath("//div[@class='property-details-sidebar-agent__sticky']//a[contains(@href,'tel')]/@href").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.split(":")[-1].strip())
        item_loader.add_xpath("landlord_name", "//div[@class='property-details-sidebar-agent__address']/strong//text()")
           
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and ("student" in p_type_string.lower()):
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower() or "shared" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower() or "room" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None