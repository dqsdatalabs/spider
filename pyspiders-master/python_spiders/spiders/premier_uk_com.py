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
    name = 'premier_uk_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.' 
    def start_requests(self):

        formdata = {
            "sortorder": "price-desc",
            "RPP": "12",
            "OrganisationId": "eeb549bf-8518-442c-a316-f24893dab9c7",
            "WebdadiSubTypeName": "Rentals",
            "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}",
            "includeSoldButton": "true",
            "page": "1",
            "incsold": "true",
        }  
        yield FormRequest(
            url="https://www.premieroxford.co.uk/api/set/results/grid",
            callback=self.parse,
            formdata=formdata,
        )
    
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'property-status to-let')]/../following-sibling::div/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            formdata = {
                "sortorder": "price-desc",
                "RPP": "12",
                "OrganisationId": "eeb549bf-8518-442c-a316-f24893dab9c7",
                "WebdadiSubTypeName": "Rentals",
                "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}",
                "includeSoldButton": "true",
                "page": str(page),
                "incsold": "true",
            }   
            yield FormRequest(
                url="https://www.premieroxford.co.uk/api/set/results/grid",
                callback=self.parse,
                formdata=formdata,
                meta={"page":page+1}
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        property_type = response.xpath("//li[contains(.,'Student Property')]").get()
        if property_type:
            item_loader.add_value("property_type", "student_apartment")
        else:        
            p_type = response.url.split("/")[-2]        
            if p_type and ("apartment" in p_type.lower() or "flat" in p_type.lower() or "maisonette" in p_type.lower()):
                item_loader.add_value("property_type", "apartment")
            elif p_type and "studio" in p_type.lower():
                item_loader.add_value("property_type", "studio")       
            elif p_type and "house" in p_type.lower():
                item_loader.add_value("property_type", "house")
            elif p_type and "student" in p_type.lower():
                item_loader.add_value("property_type", "student_apartment")
            else:
                return

        item_loader.add_value("external_source", "Premier_Uk_PySpider_united_kingdom")
        
        title = response.xpath("//section[@id='description']//h2/text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())

        zipcode = response.xpath("//span[@class='displayPostCode']//text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        city = response.xpath("//span[@class='city']//text()").extract_first()
        if city:
            item_loader.add_value("city", city.replace(",","").strip())

        address ="".join(response.xpath("//div[contains(@class,'property-address')]//text()[normalize-space()]").extract())
        if address:        
            item_loader.add_value("address", address.strip())        
 
        room_count = response.xpath("//ul[@class='FeaturedProperty__list-stats']/li[img[contains(@src,'bed')]]/span//text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif "studio" in response.url:
            item_loader.add_value("room_count", "1")
        
        bathroom_count=response.xpath("//ul[@class='FeaturedProperty__list-stats']/li[img[contains(@src,'bathroom')]]/span//text()[.!='0']").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = " ".join(response.xpath("//div[contains(@class,'property-price')]//text()[normalize-space()][not(contains(.,'Price on'))]").extract())
        if rent:
            item_loader.add_value("rent_string", rent)    
       
        desc = " ".join(response.xpath("//section[@id='description']//p/text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())         
                                          
        map_coordinate = response.xpath("//section[@id='maps']/@data-cords").extract_first()
        if map_coordinate:
            latitude = map_coordinate.split('"lat": "')[1].split('",')[0].strip()
            longitude = map_coordinate.split('"lng": "')[1].split('"}')[0].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

        images = [response.urljoin(x) for x in response.xpath("//div[@id='propertyDetailsGallery']//div/@data-bg").extract()]
        if images:
            item_loader.add_value("images", images)   

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'image-wrapper-floorplan-lightbox')]//img/@data-src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images) 
     
        elevator = response.xpath("//div[@id='collapseOne']//ul/li[contains(.,'Lift') or contains(.,'lift') ]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True) 
        terrace = response.xpath("//div[@id='collapseOne']//ul/li[contains(.,'Terrace') or contains(.,'terrace') ]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)  
        balcony = response.xpath("//div[@id='collapseOne']//ul/li[contains(.,'balcony') or contains(.,'Balcony') ]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)        
        parking = response.xpath("//div[@id='collapseOne']//ul/li[contains(.,'Garage') or contains(.,'Parking') or contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True) 
            
        pets = response.xpath("//div[@id='collapseOne']//ul/li[contains(.,'Pets') or contains(.,'pets') ]//text()").get()
        if pets:
            if "no" in pets.lower():
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)

        ext_id = response.xpath("//div[@id='collapseOne']//ul/li[contains(.,'Reference') or contains(.,'Ref:') ]//text()").get()
        if ext_id:
            if ":" in ext_id:
                ext_id = ext_id.split(":")[1]
            elif "-" in ext_id:
                ext_id = ext_id.split("-")[1]
            item_loader.add_value("external_id", ext_id.replace("Reference","").strip()) 
        else:
            external_id = response.xpath("//section[@id='description']//text()[contains(.,'REF')]").get()
            if external_id:
                item_loader.add_value("external_id", external_id.strip().split(' ')[-1].strip()) 

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//br/following-sibling::text()[contains(.,'Available')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().split('available')[-1].split('from')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        furnished = response.xpath("//div[@id='collapseOne']//ul/li[contains(.,'Furnished') or contains(.,'furnished') ]//text()").get()
        if furnished:
            if "furnished or unfurnished" in furnished.lower():
                pass
            elif "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_phone", "01865 792 299")
        item_loader.add_value("landlord_email", "enquiries@premier.uk.com")
        item_loader.add_value("landlord_name", "Premier Residential Lettings")  
        yield item_loader.load_item()
