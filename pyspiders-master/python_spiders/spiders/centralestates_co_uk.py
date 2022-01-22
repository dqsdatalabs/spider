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
import dateparser
class MySpider(Spider):
    name = 'centralestates_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {"url": "https://centralestates.co.uk/let/property-to-let/"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,)

    # 1. FOLLOWING
    def parse(self, response):
        
        data=response.xpath("//script[contains(.,'properties')]/text()").get()
        data_j=("["+data.split("var properties = [")[1].split("];")[0].strip()+"]")
        data_j=data_j.replace("\\","")
        
           
        jresp = json.loads(re.sub('\s{2,}', ' ', data_j))
        for item in jresp:
            item_loader = ListingLoader(response=response)
            property_type=item.get('propertyTypeName') 
            if property_type=="Flat":
                prop_type="apartment"
            elif property_type=='House':
                prop_type="house"
            else: prop_type=None
            
            if prop_type:
                item_loader.add_value("property_type", prop_type )
                follow_url = response.urljoin(item.get('url'))
                
                if item.get('numberbedrooms')!="0":
                    item_loader.add_value("room_count",str(item.get('numberbedrooms')))
                item_loader.add_value("latitude",str(item.get('latitude')))
                item_loader.add_value("longitude",str(item.get('longitude')))
                item_loader.add_value("zipcode", item.get('postCode'))
                item_loader.add_value("external_link", follow_url)
            
                yield response.follow(follow_url, self.populate_item, meta={'item': item_loader, "property_type":prop_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = response.meta.get("item")

        item_loader.add_value("external_source", "Centralestates_PySpider_" + self.country + "_" + self.locale)
        title=response.xpath("//div[@id='description']/div/h2[@class='info_title']//text()").get()
        if title:
            item_loader.add_value("title", re.sub(r'\s{2,}', ' ', title.strip()))
        
        item_loader.add_value("external_id", response.url.split("/")[4])

        address="".join(response.xpath("//div[@class='info_address']//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(',')[-2].strip())
        
        item_loader.add_value("landlord_name", "Central Estates")
        item_loader.add_value("landlord_phone", "020 7616 4500")
        item_loader.add_value("landlord_email", "enquiry@centralestates.com")

        bathroom_count = "".join(response.xpath("//div[contains(text(),'Bathroom')]/text()").getall())
        if bathroom_count:
            bath_count = ""
            if bathroom_count.count("Bathroom") == 2:
                bath_count = bathroom_count.strip().strip("-").split("- ")[1].strip().split(" ")[0]
                if bath_count.isdigit():
                    bath_count = bath_count
                elif "Second" in bath_count:
                    bath_count = "2"
            if bath_count:
                item_loader.add_value("bathroom_count", bath_count)
            else:
                bath = bathroom_count.replace("-","").strip().split(" ")[0]
                if "Bathroom" in bath:
                    bath = "1"
                if bath.isdigit():
                    item_loader.add_value("bathroom_count",bath)
        else:
            bathroom_count = "".join(response.xpath("//div[contains(text(),'bedroom')]/text()").getall())
            if bathroom_count:
                bath = bathroom_count.replace("-","").strip().split(" ")[0]
                
                item_loader.add_value("bathroom_count",bath.replace("Two","2"))
            
        rent="".join(response.xpath("//h2[@class='info_price']/span[contains(@class,'currencyvalue')]/text()").getall())
        if "." in rent:
            price=rent.split(".")[0]
        elif rent:
            price=rent.replace(",","")
        if price:
            item_loader.add_value("rent", str(int(price)*4))
            item_loader.add_value("currency", "GBP")
            
        desc="".join(response.xpath("//div[@class='description_wrapper']//text()").getall())
        if desc:
            item_loader.add_value("description",re.sub('\s{2,}', ' ', desc))
        
        if "sq.m" in desc:
            square_meters=desc.split("sq.m")[0].split("(")[-1]
            if "."in square_meters:
                item_loader.add_value("square_meters", square_meters.split(".")[0].strip())
            else: item_loader.add_value("square_meters", square_meters.strip())
        elif "sq.ft" in desc:
            
            square_meters=desc.split("sq.ft")[0].strip().split(" ")[-1].strip()
            sqm= str(int(square_meters)* 0.09290304)
            item_loader.add_value("square_meters", sqm.split(".")[0])
        
        images=[x for x in response.xpath("//div[@class='lightboxcarousel']/div/@data-image-src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        floor_plan_images=[x for x in response.xpath("//a[@class='floorplan-button']/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        available_date = "".join(response.xpath(
                    "//div[@class='feature_list']/div[contains(.,'Available')]/text()[not(contains(.,'Underground'))]").getall())
        if available_date:
            available_date=available_date.split("from")[1].strip()
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d/%m/%Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
            
        balcony=response.xpath("//div[@class='feature_list']/div[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking=response.xpath("//div[@class='feature_list']/div[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator=response.xpath("//div[@class='feature_list']/div[contains(.,'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        swimming_pool=response.xpath("//div[@class='feature_list']/div[contains(.,'Swimming')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        yield item_loader.load_item()