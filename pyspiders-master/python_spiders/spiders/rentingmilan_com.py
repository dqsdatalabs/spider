# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import headers
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
    name = 'rentingmilan_com'
    external_source = "Rentingmilan_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it' 
    start_urls = ['https://www.rentingmilan.com/index.php?option=com_jomestate&task=loadmore&tmpl=component']  # LEVEL 1

    formdata = {
        "glf": "",
        "minDuration": "",
        "startDate": "",
        "price_min": "",
        "price_max": "",
        "typeApt": "",
        "balcony": "",
        "bathrooms": "",
        "sleeps": "",
        "freeSearch": "",
        "sorting": "latest",
        "clickbtn": "filtri",
        "shops": "0",
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    }
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "0","2","4","5","7","8"
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "3","9"
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "6",
                ],
                "property_type": "studio"
            },
            {
                "url": [
                    "1",
                ],
                "property_type": "room"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                self.formdata["typeApt"] = item
                yield FormRequest(
                    url=self.start_urls[0],
                    callback=self.parse,
                    formdata=self.formdata,
                    headers=self.headers,
                    dont_filter=True,
                    meta={
                        "property_type": url.get('property_type')
                    }
                )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        if "noResult" not in data:
            for item in data:
                follow_url = response.urljoin(item["link"])
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'),'item':item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item=response.meta.get('item')

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        external_id = response.xpath("(//div/text()[contains(.,'Reference')]/parent::div/parent::li/text())[2]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        item_loader.add_value("address", item['fulladdress'])
        item_loader.add_value("latitude", item['maps_lat'])
        item_loader.add_value("longitude", item['maps_lng'])
        item_loader.add_value("rent", item['prezzo'])
        item_loader.add_value("currency", "EUR")

        city=" ".join(response.xpath("//h1//text()").get())
        if city:
            city="".join(city.split(",")[3:4])
            item_loader.add_value("city",city.replace(" ",""))

        square_meters=response.xpath("//ul[contains(@class,'uk-list uk-text-right uk-list-divider uk-margin-medium-top')]//li//div[contains(.,'Size')]//following-sibling::text()").get()
        if square_meters:
            square_meters=square_meters.split("sqm")[0]
            item_loader.add_value("square_meters",square_meters)

        description = "".join(response.xpath("//div[contains(@class,'uk-margin-large-bottom uk-width-xxlarge')]//text()").get())
        if description:
            item_loader.add_value("description",description)

        title = "".join(response.xpath("//title//text()").get())
        if title:
            item_loader.add_value("title",title)

        room_count = response.xpath("(//li/div[contains(.,'Sleeps')]/parent::li/text())[2]").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split('/')[0].strip())

        bathroom_count = "".join(response.xpath("//ul[contains(@class,'uk-list uk-text-right uk-list-divider uk-margin-medium-top')]//li//div[contains(.,'Number of bathrooms')]//following-sibling::text()").get())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        deposit = response.xpath("//li/div[contains(.,'deposit')]/parent::li/text()").get()
        if deposit:
            deposit=deposit.replace("€","").strip()
            if deposit and "month" in deposit:
                month = re.findall(r'\d+', deposit)
                deposit = response.xpath("//li/div[contains(.,'Rent ')]/parent::li/text()").get()
                deposit = deposit.replace("€","").replace(".","").strip()
                deposit = (int(deposit)*int(month[0]))
            item_loader.add_value("deposit", deposit)

        utilities = "".join(response.xpath("//li/div[contains(.,'utilities')]/parent::li/text()").getall())
        if utilities:
            utilities=utilities.replace("€","").strip()
            item_loader.add_value("utilities",utilities)

        available_date = response.xpath("(//li/div[contains(.,'Available')]/parent::li/text())[2]").get()
        if available_date:
            available_date = available_date.split('from ')[-1]
            date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        balcony = response.xpath("//span[contains(.,'Balcony')]/parent::a/span[@uk-icon='check']").get()
        if balcony:
            item_loader.add_value("balcony",True)
        else:
            item_loader.add_value("balcony",False)

        elevator = response.xpath("//span[contains(.,'Elevator')]/parent::a/span[@uk-icon='check']").get()
        if elevator:
            item_loader.add_value("elevator",True)
        else:
            item_loader.add_value("elevator",False)

        furnished = response.xpath("//span[contains(.,'Furnished')]/parent::a/span[@uk-icon='check']").get()
        if furnished:
            item_loader.add_value("furnished",True)
        else:
            item_loader.add_value("furnished",False)

        pets_allowed = response.xpath("//span[contains(.,'Pet Friendly')]/parent::a/span[@uk-icon='check']").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed",True)
        else:
            item_loader.add_value("pets_allowed",False)

        parking =response.xpath("//span[contains(.,'Car Parking')]/parent::a/span[@uk-icon='check']").get()
        if parking:
            item_loader.add_value("parking",True)
        else:
            item_loader.add_value("parking",False)

        terrace = response.xpath("//span[contains(.,'Terrace')]/parent::a/span[@uk-icon='check']").get()
        if terrace:
            item_loader.add_value("terrace",True)
        else:
            item_loader.add_value("terrace",False)

        images = [response.urljoin(x)for x in response.xpath("//div[contains(@class,'uk-cover-container uk-height-medium')]//img//@src").extract()]
        if images:
                item_loader.add_value("images",images)


        item_loader.add_value("landlord_phone", "+393313591275")
        item_loader.add_value("landlord_email", "info@rentingmilan.com")
        item_loader.add_value("landlord_name", "Renting Milan")

        yield item_loader.load_item()