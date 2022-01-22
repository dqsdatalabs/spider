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
    name = 'hogarthestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.hogarthestates.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=2&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1",
                ],
                "property_type": "apartment",
                "type": "Flats/Apartments"
            },
	        {
                "url": [
                    "https://www.hogarthestates.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=1&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1"
                ],
                "property_type": "house",
                "type": "Houses"
                
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type'), "type": url.get('type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[@class='info']/h3"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            status = item.xpath("./div[contains(@class,'status')]/text()").get()
            if "let" not in status.lower() and "under" not in status.lower():
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            formdata = {
                "__EVENTTARGET":response.xpath("//input[@id='__EVENTTARGET']/@value").get(),
                "__EVENTARGUMENT": "",
                "__LASTFOCUS":"",
                "__VIEWSTATE": response.xpath("//input[@id='__VIEWSTATE']/@value").get(),
                "__VIEWSTATEGENERATOR": response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").get(),
                "__EVENTVALIDATION": response.xpath("//input[@id='__EVENTVALIDATION']/@value").get(),
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$txtSearch": "",
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboPropertyTypeGroup": response.meta.get('type'),
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboCategory": "For Rent",
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboBedrooms": "0",
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboMinPrice": "0",
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboMaxPrice": "0",
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboStatus": "Show All",
                "ctl00$ContentPlaceHolderMain$lstSort": "Sort Highest Price",
                "ctl00$ContentPlaceHolderMain$cboPageNos": f"Page {page} of 4",
            }
            try:
                yield FormRequest(
                    response.url,
                    dont_filter=True,
                    formdata=formdata,
                    callback=self.parse,
                    meta={
                        "page": page+1,
                        "property_type": response.meta.get('property_type'),
                        "type": response.meta.get('type')
                    }
                )
            except: pass

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Hogarthestates_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("hoge")[-1])

        title = response.xpath("normalize-space(//div[contains(@class,'main')]/h2/text())").get()
        if title:
            item_loader.add_value("title", title)
            rent = title.split("-")[0].strip()
            if "pw" in rent.lower():
                rent = int(rent.split("£")[1].strip().split(" ")[0].replace(",",""))*4
                item_loader.add_value("rent", rent)
            else:
                rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
                item_loader.add_value("rent", rent)
        
        item_loader.add_value("currency", "GBP")
        
        square_meters = response.xpath("substring-before(//i[contains(@class,'area')]/following-sibling::text(),'m²')").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())
        
        room_count = response.xpath("//i[contains(@class,'bed')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        elif response.xpath("//span[contains(.,'Studio')]/text()").get():
            item_loader.add_value("room_count", "1")
        
        bathroom_count = response.xpath("//i[contains(@class,'bath')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(" ")[-1]
            item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", response.url.split("-pi-")[0].split("-")[-1].capitalize())
                
        furnished = response.xpath("//li[contains(.,'Furnished')]/text()")
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            furnished = response.xpath("//div[@class='main col-sm-12']/h2/text()[contains(.,'Furnished')]")
            if furnished:
                item_loader.add_value("furnished", True)           
        
        balcony = response.xpath("//li[contains(.,'Balcony')]/text()")
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]/text()")
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//li[contains(.,'Lift') or contains(.,'lift')]/text()")
        if elevator:
            item_loader.add_value("elevator", True)
        
        description = " ".join(response.xpath("//span[contains(@id,'Description')]//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        floor_plan_images = [x for x in response.xpath("//img[contains(@id,'FloorPlan')]//@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        images = [x for x in response.xpath("//div[@class='owl-carousel']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//iframe/@src[contains(.,'map')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('cbll=')[1].split(',')[0]
            longitude = latitude_longitude.split('cbll=')[1].split(',')[1].split('&')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "HOGARTH ESTATES")
        item_loader.add_value("landlord_phone", "02073735222")
        item_loader.add_value("landlord_email", "info@hogarthestates.co.uk")
        
        yield item_loader.load_item()