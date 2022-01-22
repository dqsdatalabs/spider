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
    name = 'kingstonscardiff_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    custom_settings = {"HTTPCACHE_ENABLED": False}

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.kingstonscardiff.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=2&Statusid=0&searchstatus=1&ShowSearch=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.kingstonscardiff.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=1&Statusid=0&searchstatus=1&ShowSearch=1",
                    "https://www.kingstonscardiff.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=3&Statusid=0&searchstatus=1&ShowSearch=1",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):
        view_state = response.xpath("//input[@id='__VIEWSTATE']/@value").get()
        event_validation = response.xpath("//input[@id='__EVENTVALIDATION']/@value").get()
        view_state_gen = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").get()
        property_type_group = response.xpath("//select[@name='ctl00$ContentPlaceHolderMain$uctPropertySearch$cboPropertyTypeGroup']//option[@selected='selected']/@value").get()
        
        max_page = response.xpath("//select[@name='ctl00$ContentPlaceHolderMain$cboPageNos']//option[@selected='selected']/@value").get()
        page = response.meta.get("page", 2)
        max_page = int(max_page.split('of')[-1].strip()) if max_page else -1

        for item in response.xpath("//div[@id='property-listing']/div/div//h3/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        # if page == 3 and property_type_group == 'Houses':
        #     with open("d", "wb") as f: f.write(response.body)
        
        if page <= max_page:
            url = response.url
            formdata = {
                '__EVENTTARGET': 'ctl00$ContentPlaceHolderMain$cboPageNos',
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': view_state,
                '__VIEWSTATEGENERATOR': view_state_gen,
                '__EVENTVALIDATION': event_validation,
                'ctl00$ContentPlaceHolderMain$uctPropertySearch$txtSearch': '',
                'ctl00$ContentPlaceHolderMain$uctPropertySearch$cboPropertyTypeGroup': property_type_group,
                'ctl00$ContentPlaceHolderMain$uctPropertySearch$cboBedrooms': '0',
                'ctl00$ContentPlaceHolderMain$uctPropertySearch$cboMinPrice': '0',
                'ctl00$ContentPlaceHolderMain$uctPropertySearch$cboMaxPrice': '0',
                'ctl00$ContentPlaceHolderMain$uctPropertySearch$cboStatus': 'Just Available',
                'ctl00$ContentPlaceHolderMain$lstSort': 'Sort Lowest Beds',
                'ctl00$ContentPlaceHolderMain$cboPageNos': f'Page {page} of {max_page}',
            }
            headers = {
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0',
                'Upgrade-Insecure-Requests': '1',
                'Origin': 'https://www.kingstonscardiff.co.uk',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-User': '?1',
                'Sec-Fetch-Dest': 'document',
                'Referer': 'https://www.kingstonscardiff.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=1&PriceMin=0&PriceMax=0&Bedrooms=0&Statusid=0&searchstatus=1&ShowSearch=1',
                'Accept-Language': 'tr,en;q=0.9',
                'Cookie': '_ga=GA1.3.179445081.1613019792; ASP.NET_SessionId=55cq2lynabxzbrf1eyt04nzd; _gid=GA1.3.1904839166.1613365373'
            }
            yield FormRequest(
                url,
                formdata=formdata,
                headers=headers,
                dont_filter=True,
                callback=self.parse,
                meta={"property_type": response.meta["property_type"], "page": page + 1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath('external_id','//span[contains(@id,"PropertyID")]/text()')
        item_loader.add_value("external_source", "Kingstonscardiff_Co_PySpider_united_kingdom")
        title = response.xpath("//div[contains(@class,'row')]//h2//text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        address = "".join(response.xpath("//h1//text()").getall())
        if address:
            address = address.strip().strip(",")
            item_loader.add_value("address", address.strip())
            zipcode = address.strip().split(",")[-1].strip()
            if " " in zipcode and not zipcode.split(" ")[0].isalpha():
                item_loader.add_value("zipcode", zipcode)
                city = address.split(",")[-2]
                item_loader.add_value("city", city.strip())
            else:
                item_loader.add_value("city", zipcode.strip())
        
        room_count = response.xpath("//i[contains(@class,'bedroom')]//parent::li//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//li/i[contains(@class,'bath')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//div[contains(@id,'Price')]//span[contains(.,'£')]/text()").get()
        if rent:
            rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        terrace = response.xpath("//li[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        furnished = response.xpath("//li[contains(.,' furnished') or contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True) 
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(.,'balcony') or contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//span[contains(@id,'Description')]//text()[contains(.,'Available')][not(contains(.,'Student'))]").getall())
        if available_date:
            available_date = available_date.split("Available")[1].strip()
            if "now" in available_date.lower() or "immediately" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                available_date = available_date.lower().replace("from", "").replace("*","").strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        desc = " ".join(response.xpath("//span[contains(@id,'Description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@id='property-detail-large']//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//iframe/@src[contains(.,'map')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('bll=')[1].split(',')[0]
            longitude = latitude_longitude.split('bll=')[1].split(',')[1].split('&cbp')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "KINGSTON RESIDENTIAL")
        item_loader.add_value("landlord_phone", "029 20409999")
        item_loader.add_value("landlord_email", "lettings@kingstonscardiff.co.uk")
        
        yield item_loader.load_item()