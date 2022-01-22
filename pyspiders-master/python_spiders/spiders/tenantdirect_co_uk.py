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
    name = 'tenantdirect_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": "https://tenantdirect.co.uk/properties?bedrooms=&price=&propertyType=apartment&type=&branch=professional", 
                "property_type": "apartment"
            },
            {
                "url": "https://tenantdirect.co.uk/properties?bedrooms=&price=&distance=&campus=&branch=students&lat=&lng=&page=1", 
                "property_type": "student_apartment"
            },
	        {
                "url": "https://tenantdirect.co.uk/properties?bedrooms=&price=&propertyType=house&type=&branch=professional", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//div[contains(@class,'mainPropertyImageWrapper')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})
            seen=True

        if page ==2 or seen:        
            f_url = response.url.replace(f"page={page-1}", f"page={page}")
            yield Request(f_url, callback=self.parse, meta={"page": page+1, 'property_type':response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        with open("debug", "wb") as f:f.write(response.body)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Tenantdirect_Co_PySpider_united_kingdom")

        external_id = "".join(response.xpath("//b[contains(.,'Ref code')]//parent::p//text()").getall())
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//div[contains(@class,'propertyViewHeader')]//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'propertyViewHeader')]//h1//text()").get()
        if address:
            if "," in address:
                if address.count(",") == 1:
                    city = address.split(",")[-1]
                else:
                    city = address.split(",")[-2]
                    zipcode = address.split(",")[-1]
                    item_loader.add_value("zipcode", zipcode)
            else:
                city = address
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())

        rent = "".join(response.xpath("//h4[contains(@class,'propertyViewTotalCost')]//text()").getall())
        if rent:
            rent = rent.split("£")[1].strip().split(" ")[0]
            item_loader.add_value("rent", rent)
        else:
            rent = "".join(response.xpath("//h1[contains(@class,'propertyDetail')]//text()").getall())
            if rent and "£" in rent:
                rent = rent.strip().split("£")[1].split(" ")[0]
                item_loader.add_value("rent", rent)
        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            rent=response.xpath("//title//text()").get()
            if rent:
                rent=rent.split(",")[-1].split("£")[-1]
                item_loader.add_value("rent",rent)

        item_loader.add_value("currency", "GBP")


        desc = " ".join(response.xpath("//span[contains(@class,'truncatedPropertyDetail')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = "".join(response.xpath("//h1[contains(@class,'propertyDetail')]//text()").getall())
        if room_count:
            if "studio" in room_count.lower():
                item_loader.add_value("room_count","1")
            else:
                room_count = room_count.strip().split(" ")[0]
                item_loader.add_value("room_count", room_count)
        bathroom_count=response.xpath("//strong[contains(.,'Bathroom')][last()]/text()").get()
        if bathroom_count:
            bath=re.findall("\d+",bathroom_count)
            item_loader.add_value("bathroom_count",bath)
        parking=response.xpath("//strong[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        latitude=response.xpath("//script[contains(.,'displayMap')]/text()").get()
        if latitude:
            latitude=latitude.split("displayMap(lat,")[-1]

            lat=latitude.split("displayMap")[-1].split(")")[0].split(",")[0].replace("(","")
            item_loader.add_value("latitude",lat)
        longitude=response.xpath("//script[contains(.,'displayMap')]/text()").get()
        if longitude:
            longitude=longitude.split("displayMap(lat,")[-1]
            lon=longitude.split("displayMap")[-1].split(")")[0].split(",")[-1].replace("-","")
            item_loader.add_value("longitude",lon)
        

        
        images = [x for x in response.xpath("//div[contains(@id,'slider')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        else:
            images = [x for x in response.xpath("//div[contains(@class,'profSliderImageWrapper')]//@src").getall()]
            if images:
                item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//b[contains(.,'Date available')]//parent::p/text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        else:
            available_date = "".join(response.xpath("//p[contains(@class,'propertyDetail-smallText')][contains(.,'Available')]//parent::h1/text()").getall())
            if available_date:
                if not "now" in available_date.lower():
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
        datecheck=item_loader.get_output_value("available_date")
        if not datecheck:
                available_datee=response.xpath("//h1[@class='propertyDetail']//text()").getall()
                if available_datee:
                    available_date=available_datee[-1].strip()
                if not "now" in available_date.lower():
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)


            

        furnished = response.xpath("//b[contains(.,'Furnishing')]//parent::p//text()[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_name", "Tenant Direct")
        item_loader.add_value("landlord_phone", "023 8033 2230")
        
        yield item_loader.load_item()