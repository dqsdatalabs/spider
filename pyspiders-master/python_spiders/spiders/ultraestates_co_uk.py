# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re
class MySpider(Spider):
    name = 'ultraestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    post_url = ['http://www.ultraestates.co.uk/search.vbhtml?properties-to-rent']  # LEVEL 1

    form_data = {
        "salerent":"nr",
        "area":"" ,
        "type": "Flat/Apartment",
        "minbaths":"" ,
        "prostu":"" ,
        "minbeds":"" ,
        "minprice": "",
        "maxprice": "",
        "links": "1",
        "PropPerPage": "48",
        "order": "high",
        "radius": "0",
        "grid": "grid",
        "search": "yes",
    }
    
    def start_requests(self):
        start_urls = [
            {
                "type": ["Flat/Apartment", "Garden Flat"],
                "property_type": "apartment"
            },
	        {
                "type": ["House", "Penthouse", "Duplex"],
                "property_type": "house"
            },
            {
                "type": ["Studio"],
                "property_type": "studio"
            }
           
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('type'):
                self.form_data["type"] = item
                yield FormRequest(
                    url=self.post_url[0],
                    callback=self.parse,
                    formdata=self.form_data,
                    meta={'property_type': url.get('property_type'),"page":1,"type":item}
                )
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='propertylight']/div/a/@href").extract():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//li/a[.='Next']/text()").get()
        if next_page:
            page = response.meta.get('page')
            self.form_data["links"] = str(page+1)
            self.form_data["type"] = response.meta.get('type')
            yield FormRequest(
                url=self.post_url[0],
                callback=self.parse,
                formdata=self.form_data,
                meta={"property_type": response.meta.get('property_type'),"page":page+1,"type":response.meta.get('type') }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Ultraestates_Co_PySpider_united_kingdom")
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1].strip())
        
        rent = response.xpath("//span[@class='fullprice2']/text()").get()
        if rent:
            rent = int(rent.replace("£","").replace(",",""))*4
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        rooms = response.xpath("//p[@class='photos-pad']/text()").get()
        if rooms:
            if "Bedroom" in rooms:
                item_loader.add_value("room_count", rooms.split("Bed")[0].strip().split(" ")[-1])
            if "Bathroom" in rooms:
                item_loader.add_value("bathroom_count", rooms.split("Bath")[0].strip().split(" ")[-1])
        
        description = " ".join(response.xpath("//p[@class='lead']//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))

        square_meters = " ".join(response.xpath("//text()[contains(.,'sq')]").getall())
        if square_meters: 
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",square_meters.replace(",",""))
            if unit_pattern:
                sqm = str(int(float(unit_pattern[0][0]) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
        
        item_loader.add_xpath("external_id","substring-after(//p[contains(.,'Reference')]//text(),':')")
        
        import dateparser
        available_date = response.xpath("//p[@class='photos-pad']/text()[2]").get()
        if available_date and "now" not in available_date.lower():
            date_parsed = dateparser.parse(available_date.split("Available")[1].strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        energy_label = response.xpath("substring-before(substring-after(//img/@src[contains(.,'epc')],'epc1='),'&')").get()
        if energy_label and energy_label !='0':
            item_loader.add_value("energy_label", energy_label)

        elevator = response.xpath("//li[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//li[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//li[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floorplan']//@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        images = [response.urljoin(x) for x in response.xpath("//section[contains(@class,'propertyfullpage')]//@src[contains(.,'property')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Ultra Estates")
        item_loader.add_value("landlord_phone", "0207 723 4288")
        item_loader.add_value("landlord_email", "enquiries@ultraestates.co.uk")
        
        yield item_loader.load_item()