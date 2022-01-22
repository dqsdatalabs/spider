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
    name = 'rawmarrickville_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ['https://marrickville.randw.com.au/property.html']  # LEVEL 1
    
    formdata = {
        "act": "act_fgxml",
        "31[offset]": "6",
        "31[perpage]": "6",
        "SalesStageID[0]": "LISTED_PT",
        "SalesStageID[1]": "LISTED_LEASE",
        "SalesStageID[2]": "LISTED_AUCTION",
        "SalesStageID[3]": "DEPOSIT",
        "SalesStageID[4]": "BOND",
        "SalesStageID[5]": "EXCHANGED_UNREPORTED",
        "SalesCategoryID[0]": "RESIDENTIAL_LEASE",
        "SalesCategoryID[1]": "RURAL_LEASE",
        "SalesCategoryID[2]": "COMMERCIAL_LEASE",
        "Address": "",
        "RegionID": "",
        "require": "0",
        "fgpid": "31",
        "ajax": "1",
    }
    
    def start_requests(self):
        yield FormRequest(
            url=self.start_urls[0],
            formdata=self.formdata,
            callback=self.parse,
            meta={'property_type': "apartment"}
        )

    # 1. FOLLOWING
    def parse(self, response):
        print(response.body)
        
        page = response.meta.get('page', 6)
        seen = False
        for item in response.xpath("//rows/row"):
            follow_url = response.urljoin(item.xpath("./url/text()").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "item": item})
            seen = True
        if page == 2 or seen:
            url = "https://marrickville.randw.com.au/property.html"
            self.formdata["31[offset]"] = str(page)
            yield FormRequest(url, dont_filter=True, formdata=self.formdata, callback=self.parse, meta={"page": page+6,})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        description = " ".join(response.xpath("//div[@class='descview-inner']/text()").getall())
        if "house" in description.lower() or "semi-detached" in description.lower():
            item_loader.add_value("property_type", "house")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_source", "Rawmarrickville_PySpider_australia")
        
        item_loader.add_xpath("title", "//h2[@class='heading']/text()")
        item = response.meta.get('item')
        city = item.xpath("//Suburb/text()").get()
        address = item.xpath("//Address/text()").get()
        item_loader.add_value("address", f"{address} {city}".strip())
        item_loader.add_value("city", city)
        
        features = item.xpath("//features/text()").get()
        if "bed" in features: item_loader.add_value("room_count", features.split("bed")[0].strip())
        if "bath" in features: item_loader.add_value("bathroom_count", features.split("bath")[0].strip().split(" ")[-1])
        if "car" in features:
            parking = features.split("car")[0].strip().split(" ")[-1]
            if parking !='0': item_loader.add_value("parking", True)
        
        rent = item.xpath("//DisplayPrice/text()").get()
        print(rent)
        if rent and not "1 WEEKS FREE RENT" in rent:
            rent = int(float(rent.lower().split(" per ")[0].strip().split(" ")[-1].replace("$","").replace(",", "")))*4
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")
        
        external_id = item.xpath("//id/text()").get()
        item_loader.add_value("external_id", external_id)
        
        if description:
            item_loader.add_value("description", description.strip())
        
        deposit = response.xpath("//div[@class='label' and contains(.,'Bond')]/following-sibling::div[1]/text()").get()
        if deposit:
            deposit = deposit.replace("$","").replace(",","")
            item_loader.add_value("deposit", int(float(deposit)))
        
        import dateparser
        available_date = response.xpath("//div[@class='label' and contains(.,'Available')]/following-sibling::div[1]/text()").get()
        if available_date and "now" not in available_date.lower():
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//div[@class='image image-link image-fluid image-property']//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        name = response.xpath("//div[contains(@class,'light-inner')]/h3[@class='heading']/text()").get()
        if name: item_loader.add_value("landlord_name", name)
        
        phone = response.xpath("//div[1][contains(@class,'light-inner')]//td/a/text()").get()
        if phone: item_loader.add_value("landlord_phone", phone)
        else: item_loader.add_value("landlord_phone", "(02) 9518 1655")
        
        item_loader.add_value("landlord_email", "marrickville@randw.com.au")
        javascript = response.xpath("//script[@type='application/ld+json' and contains(.,'postalCode')]/text()").get()
        if javascript: 
            data = json.loads(javascript)
            zipcode = data["address"]["addressRegion"]+" "+data["address"]["postalCode"]
            item_loader.add_value("zipcode", zipcode)
        javascript = response.xpath("//script[contains(.,'POINT(')]/text()").extract_first()
        if javascript:        
            latitude = javascript.split("POINT(")[1].split(",")[0]
            longitude = javascript.split("POINT(")[1].split(",")[1].split(")")[0]
            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)
        yield item_loader.load_item()