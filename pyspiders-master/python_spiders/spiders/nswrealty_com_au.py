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
    name = 'nswrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        queries = [
            {
                "query": ["Unit","Apartment","Flat",],
                "property_type": "apartment",
            },
            {
                "query": ["House","Townhouse","Villa","DuplexSemi-detached","Terrace",],
                "property_type": "house",
            },
            {
                "query": ["Studio",],
                "property_type": "studio",
            },
        ]
        start_url = "https://www.nswrealty.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bpost_type%5D%5Btype%5D=equal&query%5Bcount%5D%5Bvalue%5D=20&query%5Bcount%5D%5Btype%5D=equal&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bmeta_key%5D%5Btype%5D=equal&query%5Bsold%5D%5Bvalue%5D=0&query%5Bsold%5D%5Btype%5D=equal&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Border%5D%5Bvalue%5D=dateListed-desc&query%5Border%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D=1&query%5Bextended%5D%5Bvalue%5D=1&query%5Bminprice%5D%5Bvalue%5D=&query%5Bmaxprice%5D%5Bvalue%5D=&query%5Bminbeds%5D%5Bvalue%5D=&query%5Bmaxbeds%5D%5Bvalue%5D=&query%5Bminbaths%5D%5Bvalue%5D=&query%5Bmaxbaths%5D%5Bvalue%5D=&query%5Bcars%5D%5Bvalue%5D=&query%5Btype%5D%5Bvalue%5D=&query%5Bsubcategory%5D%5Bvalue%5D={}&query%5BexternalID%5D%5Bvalue%5D=&query%5Blandsize%5D%5Bvalue%5D=&query%5Blandareaunit%5D%5Bvalue%5D="
        for item in queries:
            for query in item.get("query"):
                yield FormRequest(start_url.format(query),
                            callback=self.parse,
                            meta={'property_type': item.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        for item in data["data"]["listings"]:
            seen = True
            follow_url = item["url"]
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"],"item":item})

        if page == 2 or seen: 
            f_url = response.url.replace("query%5Bpaged%5D%5Bvalue%5D=" + str(page - 1), "query%5Bpaged%5D%5Bvalue%5D=" + str(page))
            yield FormRequest(f_url, callback=self.parse, meta={'property_type':response.meta["property_type"], "page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Nswrealty_Com_PySpider_australia")
        
        title = response.xpath("//p[contains(@class,'single-listing-address')]/text()").get()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address", title)
            item_loader.add_value("city", title.split(",")[-1].strip())
        
        rent = response.xpath("//p[@class='listing-info-price']/text()").get()
        if rent:
            price = rent.split(" ")[0].replace("$","")
            try:
                item_loader.add_value("rent", int(float(price))*4)
            except: pass
        item_loader.add_value("currency", "AUD")
        
        room_count = response.xpath("//p[contains(@class,'bed')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        room_count = response.xpath("//p[contains(@class,'bath')]/text()").get()
        if room_count:
            item_loader.add_value("bathroom_count", room_count)
        
        parking = response.xpath("//p[contains(@class,'car')]/text()[.!='0'] | //li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        deposit = response.xpath("//strong[contains(.,'bond')]/following-sibling::text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("$","").strip())
        
        external_id = response.xpath("//strong[contains(.,'ID')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        balcony = response.xpath("//li[contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng:')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        import dateparser
        available_date = response.xpath("//strong[contains(.,'date available')]/following-sibling::text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        description = " ".join(response.xpath("//div[contains(@class,'post-content')]//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x.split("'")[1] for x in response.xpath("//div[contains(@class,'slides')]//@style[contains(.,'url')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "NSW REALTY")
        item_loader.add_value("landlord_phone", "02 8756 5444")
        item_loader.add_value("landlord_email", "home@nswre.com.au")
        
        yield item_loader.load_item()