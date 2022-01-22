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
import re
class MySpider(Spider):
    name = 'spencer_james_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ['https://www.spencer-james.co.uk/properties?eapowquicksearch=1&limitstart=0']  # LEVEL 1
    
    def start_requests(self):
        start_urls = [
            {
                "url": ["5", "6"],
                "property_type": "apartment"
            },
	        {
                "url": ["3", "4"],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                formdata = {
                    "filter_cat": "2",
                    "tx_placename": "",
                    "filter_rad": "5",
                    "filter_keyword": "",
                    "filter_beds": "",
                    "eapow-qsmod-types": item,
                    "selectItemeapow-qsmod-types": item,
                    "filter_price_low": "",
                    "filter_price_high": "",
                    "commit": "",
                    "filter_lat": "0",
                    "filter_lon": "0",
                    "filter_location": "[object Object]",
                    "filter_types": item,
                }
                yield FormRequest(
                    url=self.start_urls[0],
                    dont_filter=True,
                    formdata=formdata,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type'), "formdata": formdata}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 10)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'eapow-property-thumb-holder')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 10 or seen:
            url = f"https://www.spencer-james.co.uk/properties?eapowquicksearch=1&start={page}"
            formdata = response.meta.get('formdata')
            yield FormRequest(
                url,
                dont_filter=True,
                formdata=formdata,
                callback=self.parse,
                meta={"page": page+10, "property_type": response.meta.get('property_type'), "formdata": formdata}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Spencer_James_Co_PySpider_united_kingdom")
        external_id = response.xpath("//div[b[.='Ref #']]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.replace(": ",""))    
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", title.strip())    
        features = " ".join(response.xpath("//li[contains(.,'Area')]//text()").getall())
        if features:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",features.replace(",",""))
            if unit_pattern:
                item_loader.add_value("square_meters", str(int(float(unit_pattern[0][0]) * 0.09290304)))   
        item_loader.add_xpath("room_count","//div/i[@class='flaticon-bed']/following-sibling::strong[1]/text()")
        item_loader.add_xpath("bathroom_count","//div/i[@class='flaticon-bath']/following-sibling::strong[1]/text()")
        item_loader.add_xpath("rent_string","//h1/small[@class='eapow-detail-price']/text()")
        address = ", ".join(response.xpath("//div[@id='DetailsBox']//address//text()").getall())

        available_date=response.xpath("//ul[@id='starItem']/li/text()[contains(.,'Available')]").get()

        if available_date:
            date2 =  available_date.split(" ")[-1].strip()
            if "Now" not in date2 and "Cost" not in date2:
                date_parsed = dateparser.parse(
                    date2, date_formats=["%m"]
                )
                if date_parsed:
                    date3 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date3)

        if address:
            item_loader.add_value("address", address.strip())    
        city_zip = response.xpath("//div[@id='DetailsBox']//address/text()[last()]").get()
        if city_zip:
            item_loader.add_value("zipcode", " ".join(city_zip.strip().split(" ")[-2:]) )
            item_loader.add_value("city", " ".join(city_zip.strip().split(" ")[:-2]) )
        description = " ".join(response.xpath("//div[@id='propdescription']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//div[@id='eapowgalleryplug']//div//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@id='eapowfloorplanplug']//div//a/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
   
        item_loader.add_xpath("landlord_phone", "//div[@id='DetailsBox']//div[contains(.,'P:')]/text()")     
        item_loader.add_value("landlord_email", "mail@spencer-james.co.uk")
        item_loader.add_xpath("landlord_name", "//div[@id='DetailsBox']//div[@class='span8']//a[1]//text()")
        furnished = response.xpath("//ul[@id='starItem']/li/text()[contains(.,'Furnish') or contains(.,'furnish')]").get()
        if furnished:
            if "unfurnish" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnish" in furnished.lower():
                item_loader.add_value("furnished", True)
        balcony = response.xpath("//ul[@id='starItem']/li/text()[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)  
        parking = response.xpath("//ul[@id='starItem']/li/text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True) 
        lat_lng = response.xpath("//script[contains(.,'lat:') and contains(.,'lon:')]/text()").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split('lat: "')[-1].split('"')[0])
            item_loader.add_value("longitude", lat_lng.split('lon: "')[-1].split('"')[0])
        yield item_loader.load_item()