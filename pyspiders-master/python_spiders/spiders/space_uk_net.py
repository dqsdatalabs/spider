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

class MySpider(Spider):
    name = 'space_uk_net'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    headers = {
            "content-type": "application/x-www-form-urlencoded",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "origin": "https://www.space.uk.net"
    }

    def start_requests(self):
        start_urls = [
            {"url": "https://www.space.uk.net/properties.aspx?Mode=1&PropertyTypeGroup=2&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1", "property_type": "apartment"},
	        {"url": "https://www.space.uk.net/properties.aspx?Mode=1&PropertyTypeGroup=1&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        seen = False
        for item in response.xpath("//div[@id='property-listing']//h3/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        viewstate = response.xpath("//input[@id='__VIEWSTATE']/@value").extract_first()
        event = response.xpath("//input[@id='__EVENTVALIDATION']/@value").extract_first()
        generator = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").extract_first()
        prop_type = response.xpath("//select[@name='ctl00$ContentPlaceHolderMain$uctPropertySearch$cboPropertyTypeGroup']/option[@selected]/@value").extract_first()
        page = response.xpath("//select[@name='ctl00$ContentPlaceHolderMain$cboPageNos']/option[@selected]/@value").extract_first()
        
        data = {
            "__EVENTTARGET": "ctl00$ContentPlaceHolderMain$lnkPageNext",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": generator,
            "__EVENTVALIDATION": event,
            "ctl00$ContentPlaceHolderMain$uctPropertySearch$txtSearch": "",
            "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboPropertyTypeGroup": prop_type,
            "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboCategory": "For Rent",
            "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboBedrooms": "0",
            "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboMinPrice": "0",
            "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboMaxPrice": "0",
            "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboStatus": "Show All",
            "ctl00$ContentPlaceHolderMain$lstSort": "Sort Highest Price",
            "ctl00$ContentPlaceHolderMain$cboPageNos": page
        }

        if page != "Page 1 of 1" and seen:
            yield FormRequest(
                response.url,
                formdata=data,
                headers=self.headers,
                callback=self.parse,
                meta={"property_type":property_type}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_value("external_link", response.url)
        
        item_loader.add_value("external_source", "Space_Uk_PySpider_united_kingdom")

        banner_title = response.xpath("//h1[@id='banner-title']/text()").get()
        if banner_title:
            if not banner_title.split(",")[-1].strip().isalpha():
                city = banner_title.split(",")[-2].strip()
            else:
                city = banner_title.split(",")[-1].strip()
            item_loader.add_value("city", city)

        external_id = response.xpath("//span[contains(@id,'PropertyID')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.split(',')[-1].split(" - ")[0].strip().replace(".","")
            if "Reading" not in zipcode:
                item_loader.add_value("zipcode",zipcode )

        description = " ".join(response.xpath("//div[@id='tabDescription']/p//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//i[@class='icon-area']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.split('m')[0].split('/')[-1].strip()))))

        room_count = response.xpath("//i[@class='icon-bedrooms']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//i[@class='icon-bathrooms']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//h2/text()").get()
        if rent:
            if ' pw' in rent:
                rent = rent.split('£')[-1].split(' pw')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            elif ' pcm' in rent:
                rent = rent.split('£')[-1].split(' pcm')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))
                item_loader.add_value("currency", 'GBP')

        available_date = response.xpath("//h2/text()[contains(.,'Available')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().split('available')[-1].split('-')[0].strip(), date_formats=["%d %B %Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-detail-large']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor_plan_images = [x for x in response.xpath("//div[@id='tabFloorPlan']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        latitude_longitude = response.xpath("//iframe[contains(@src,'maps?')]/@src").get()
        if latitude_longitude:
            item_loader.add_value("latitude", latitude_longitude.split('cbll=')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude_longitude.split('cbll=')[1].split(',')[1].split('&')[0].strip())
        
        parking = response.xpath("//li[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//h2/text()").get()
        if furnished:
            if furnished.split('-')[-1].strip().lower() == 'unfurnished':
                item_loader.add_value("furnished", False)
            elif 'furnished' in furnished.split('-')[-1].strip().lower() and not '/' in furnished.split('-')[-1].strip().lower():
                item_loader.add_value("furnished", True)
        else:
            furnished = response.xpath("//li[contains(.,'Furnished')]").get()
            if furnished:
                item_loader.add_value("furnished", True)

        terrace = response.xpath("//span[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_phone", "01189 66 66 60")
        item_loader.add_value("landlord_email", "lets@space.uk.net")
        item_loader.add_value("landlord_name", "Space Lettings")

        yield item_loader.load_item()