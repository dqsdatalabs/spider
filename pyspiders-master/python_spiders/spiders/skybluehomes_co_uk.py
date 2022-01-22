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
    name = 'skybluehomes_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator=','
    scale_separator='.'
    def start_requests(self):
        start_urls = [
            {
                "type" : "4",
                "property_type" : "house"
            },
            {
                "type" : "5",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:

            formdata = {
                "tx_placename": "",
                "filter_rad": "5",
                "eapow-qsmod-types": url.get("type"),
                "selectItemeapow-qsmod-types": url.get("type"),
                "filter_keyword": "",
                "filter_beds": "",
                "filter_baths": "",
                "filter_price_low": "",
                "filter_price_high": "",
                "commit": "",
                "filter_lat": "0",
                "filter_lon": "0",
                "filter_location": "[object Object]",
                "filter_types": url.get("type"),
            }

            yield FormRequest(
                url="https://www.skybluehomes.co.uk/properties?eapowquicksearch=1&limitstart=0",
                callback=self.parse,
                formdata=formdata,
                meta={'property_type': url.get('property_type')}
            )

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 12)

        seen = False
        for item in response.xpath("//div[@class='eapow-overview-short-desc']//a[contains(.,'Read')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 12 or seen:
            p_url = f"https://www.skybluehomes.co.uk/properties?eapowquicksearch=1&start={page}"
            yield Request(
                url=p_url,
                callback=self.parse,
                dont_filter=True,
                meta={'property_type': response.meta.get('property_type'), "page":page+12}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Skybluehomes_PySpider_"+ self.country + "_" + self.locale)
        rented = response.xpath("//div[@class='eapow-bannertopright']/img/@alt").extract_first()     
        if rented:
            if "let stc" in rented.lower():
                return
        title = response.xpath("//div[@class='eapow-mainheader']/h1/text()[normalize-space()]").extract_first()     
        if title:   
            item_loader.add_value("title",title.strip()) 
        external_id = response.xpath("substring-after(//div[@class='eapow-sidecol'][contains(.,'Ref')]/text(),':')").extract_first()     
        if external_id:   
            item_loader.add_value("external_id",external_id.strip())


        rent = response.xpath("//div[@class='eapow-mainheader']/h1/small/text()").extract_first()
        if rent:
            if "Weekly" in rent:
                numbers = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if numbers:
                    rent = int(numbers[0].replace(".",""))*4
                    rent = "£"+str(rent)
            item_loader.add_value("rent_string", rent)
        room = response.xpath("//div[@class='span4']//i[contains(@class,'bedroom')]/following-sibling::span[1]/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip())
        bathroom_count = response.xpath("//div[@class='span4']//i[contains(@class,'bathroom')]/following-sibling::span[1]/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        furnished = response.xpath("//div[@class='span12']//ul[@id='starItem']/li[contains(.,'Furnished') or contains(.,'FURNISHED')]/text()").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)

        parking = response.xpath("//div[@class='span12']//ul[@id='starItem']/li[contains(.,'GARAGE') or contains(.,'Garage')]/text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        address =", ".join(response.xpath("//div[@class='eapow-sidecol eapow-mainaddress']/address//text()").extract())
        if address:   
            item_loader.add_value("address", address.strip())
        city_zipcode = response.xpath("//div[@class='eapow-sidecol eapow-mainaddress']/address/text()").extract_first()
        if city_zipcode:
            try:
                city = city_zipcode.strip().split(" ")[0]
                item_loader.add_value("city", city.strip())
                item_loader.add_value("zipcode", city_zipcode.replace(city,"").replace("(MASTER)","").strip())
            except:
                pass
        available_date = response.xpath("//div[contains(@class,'desc-wrapper')]/p//text()[contains(.,'Available')]").extract_first()
        if available_date:  
            try:
                date_format = available_date 
                if "from" in available_date:
                    date_format = available_date.split("from")[1].replace(":","").strip()
                elif "Available" in available_date:
                    date_format = available_date.split("Available")[1].strip()
                newformat = dateparser.parse(date_format).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)
            except:
                pass
        map_coordinate = response.xpath("//script[@type='text/javascript']//text()[contains(.,'lat:') and contains(.,'lon:')]").extract_first()
        if map_coordinate:
            latitude = map_coordinate.split('lat: "')[1].split('"')[0]
            longitude = map_coordinate.split('lon: "')[1].split('"')[0]
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            
        desc = " ".join(response.xpath("//div[contains(@class,'desc-wrapper')]/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "washing machine" in desc.lower():
                item_loader.add_value("washing_machine", True)
            if "parking" in desc.lower() or "garage" in desc.lower():
                item_loader.add_value("parking", True)
            if " furnished" in desc:
                item_loader.add_value("furnished", True)



        deposit = response.xpath("//div[contains(@class,'desc-wrapper')]/p//text()[contains(.,'Deposit')]").extract_first()
        if deposit:
            deposit = deposit.split("Deposit")[1].strip().split(" ")[0].replace("£","").strip()
            if deposit.isdigit():
                item_loader.add_value("deposit", deposit.strip())
        images = [x for x in response.xpath("//div[@id='eapowgalleryplug']//div/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "0116 254 8107")
        item_loader.add_value("landlord_email", "info@skybluehomes.co.uk")
        item_loader.add_value("landlord_name", "Sky Blue Homes")
        yield item_loader.load_item()
