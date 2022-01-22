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
from word2number import w2n
import dateparser
from datetime import datetime

class MySpider(Spider):
    name = 'danielforduk_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'  
    start_urls = ["http://www.danielforduk.com/search.aspx?ListingType=6&areainformation=&areainformationname=Location&radius=0&statusids=1&igid=&imgid=&egid=&emgid=&category=1&defaultlistingtype=5&markettype=0&cur=GBP"]
    

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[@class='propAdd']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            view_state = response.meta.get("view_state", response.xpath("//input[@name='__VIEWSTATE']/@value").get())
            view_state_gen = response.meta.get("view_state_gen", response.xpath("//input[@name='__VIEWSTATEGENERATOR']/@value").get())

            formdata = {
                "ctl00$ctl14": "ctl00$cntrlCenterRegion$ctl00$cntrlSearchResultsUpdatePanel|ctl00$cntrlCenterRegion$ctl00$cntrlPagingFooter",
                "ctl00_ctl14_TSM": ";;System.Web.Extensions, Version=3.5.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35:en-US:16997a38-7253-4f67-80d9-0cbcc01b3057:ea597d4b:b25378d2;Telerik.Web.UI, Version=2012.2.724.35, Culture=neutral, PublicKeyToken=121fae78165ba3d4:en-US:c8b048d6-c2e5-465c-b4e7-22b9c95f45c5:16e4e7cd:f7645509:24ee1bba:f46195d3:874f8ea2:19620875:39040b5c:f85f9819:2003d0b8:1e771326:aa288e2d:e330518b:8e6f0d33",
                "__EVENTTARGET": "ctl00$cntrlCenterRegion$ctl00$cntrlPagingFooter",
                "__EVENTARGUMENT": str(page),
                "__LASTFOCUS": "",
                "ctl00_cntrlItemTemplateActionTooltipManager_ClientState": "",
                "ctl00$cntrlLeftRegion$ctl00$cntrlListingTypes": "6",
                "ctl00$cntrlLeftRegion$ctl00$cntrlCIPEmail$rdoEmailAlerts": "rdoEmailAlerts_No",
                "ctl00$cntrlLeftRegion$ctl00$cntrlLocationItems": "Location",
                "ctl00_cntrlLeftRegion_ctl00_cntrlLocationItems_ClientState": "",
                "ctl00$cntrlLeftRegion$ctl00$cntrlRadius": "Radius",
                "ctl00_cntrlLeftRegion_ctl00_cntrlRadius_ClientState": "",
                "ctl00$cntrlLeftRegion$ctl00$cntrlMinimumPrice": "Min Price",
                "ctl00_cntrlLeftRegion_ctl00_cntrlMinimumPrice_ClientState": "",
                "ctl00$cntrlLeftRegion$ctl00$cntrlMaximumPrice": "Max Price",
                "ctl00_cntrlLeftRegion_ctl00_cntrlMaximumPrice_ClientState": "",
                "ctl00$cntrlLeftRegion$ctl00$cntrlMinimumBedrooms": "Minimum Bedrooms",
                "ctl00_cntrlLeftRegion_ctl00_cntrlMinimumBedrooms_ClientState": "",
                "ctl00_cntrlCenterRegion_ctl00_cntrlSavedItems_cntrlTooltipManager_ClientState": "",
                "ctl00$cntrlCenterRegion$ctl00$ctl02$priceordering": "cntrlPriceAscending",
                "ctl00$cntrlCenterRegion$ctl00$ctl02$cntrlPrice": "Price (Low to High)",
                "ctl00_cntrlCenterRegion_ctl00_ctl02_cntrlPrice_ClientState": "",
                "ctl00$cntrlCenterRegion$ctl00$ctl02$cntrlItemsPerPage": "6",
                "ctl00_cntrlCenterRegion_ctl00_ctl02_cntrlItemsPerPage_ClientState": "",
                "ctl00_cntrlCenterRegion_ctl00_ctl03_ctl03_cntrlSavedProperties_cntrlSavedItems_cntrlTooltipManager_ClientState": "",
                #"ctl00_cntrlCenterRegion_ctl00_ctl03_ctl03_cntrlSavedProperties_cntrlSavedPropertiesTabStrip_ClientState": '{"selectedIndexes":[],"logEntries":[],"scrollState":\{\}}',
                "ctl00$cntrlCenterRegion$ctl00$ctl03$ctl03$cntrlSavedProperties$ctl02$ctl03$rptItems$ctl00$cntrlRating$txtItemValue": "5",
                "ctl00$cntrlCenterRegion$ctl00$ctl03$ctl03$cntrlSavedProperties$ctl02$ctl03$rptItems$ctl01$cntrlRating$txtItemValue": "4",
                "ctl00$cntrlCenterRegion$ctl00$ctl03$ctl04$rptItems$ctl00$cntrlRating$txtItemValue": "5",
                "ctl00$cntrlCenterRegion$ctl00$ctl03$ctl04$rptItems$ctl01$cntrlRating$txtItemValue": "4",
                "mapdraggable": "0",
                "ctl00$cntrlCenterRegion$ctl00$cntrlMapView$txtFunction": "",
                "ctl00$cntrlCenterRegion$ctl00$cntrlMapView$txtCallBackFunction": "",
                "ctl00$cntrlCenterRegion$ctl00$cntrlMapView$txtData1": "",
                "ctl00$cntrlCenterRegion$ctl00$cntrlMapView$txtData2": "",
                "ctl00_cntrlCenterRegion_ctl00_cntrlMapView_cntrlMapSavedItems_cntrlTooltipManager_ClientState": "",
                "ctl00$ctl17$ctl02$rptItems$ctl00$cntrlRating$txtItemValue": "5",
                "ctl00$ctl17$ctl02$rptItems$ctl01$cntrlRating$txtItemValue": "4",
                "ctl00$ctl17$ctl02$rptItems$ctl02$cntrlRating$txtItemValue": "5",
                "__VIEWSTATEGENERATOR": view_state_gen,
                "__VIEWSTATE": view_state,
                "__ASYNCPOST": "true",
            }

            yield FormRequest(
                url="http://www.danielforduk.com/search.aspx?ListingType=6&areainformation=&areainformationname=Location&radius=0&statusids=1&igid=&imgid=&egid=&emgid=&category=1&defaultlistingtype=5&markettype=0&cur=GBP",
                callback=self.parse,
                formdata=formdata,
                meta={"page":page+1, "view_state":view_state, "view_state_gen":view_state_gen}
            )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Danielforduk_PySpider_"+ self.country + "_" + self.locale)

        summary = "".join(response.xpath("//span[contains(@id,'FullDescription')]//text()").extract())
        prop_type = get_prop_type(summary)
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            features = " ".join(response.xpath("//div[@class='PropertyFeatures']//li/text()").extract())
            prop_type = get_prop_type(features)
            if prop_type:
                item_loader.add_value("property_type", prop_type)
            else:
                return
            
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        floor = response.xpath("//span[@class='bulleted-list']//ul/li[contains(.,'Floor')]//text()[not(contains(.,'Wooden'))]").get()
        
        if floor:

            item_loader.add_value("floor", floor.split("Floor")[0].strip())

        
        address = response.xpath("//h1[contains(@id,'Address')]/text()").get()
        if address:
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//span[contains(@id,'Price')]/text()").get()
        if rent and "P.O.A" not in rent:
            price = rent.split(" ")[0].split("£")[1].replace(",","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//li[contains(.,'bedroom')]/text()").get()
        room = response.xpath("//span//li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            room_count = room_count.replace("double","").replace("spacious","").split("bedroom")[0].strip()
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            elif room_count:
                room_count = w2n.word_to_num(room_count)
                item_loader.add_value("room_count", room_count)
        elif room:
            room = room.replace("Brand New","")
            if room.strip().split(" ")[0].isdigit():
                item_loader.add_value("room_count", room.strip().split(" ")[0])
            elif room.strip().split(" ")[1].isdigit():
                item_loader.add_value("room_count", room.strip().split(" ")[1])
            else:
                room = room.strip().split(" ")[0]
                if "Double" in room:
                    item_loader.add_value("room_count", "1")
                else:
                    item_loader.add_value("room_count", w2n.word_to_num(room))            
            
        bathroom = response.xpath("//li[contains(.,'Bathroom') or contains(.,'bathroom')]/text()").get()
        bathroom_count = False
        if bathroom and bathroom.lower() != "modern" and "beautiful" not in bathroom.lower():
            bathroom = bathroom.strip().lower().split("bathroom")[0].strip()
            if " " in bathroom:
                bathroom_count = bathroom.split(" ")[0]
            else:
                bathroom_count = bathroom
            if bathroom_count:
                if bathroom_count.isdigit():
                    item_loader.add_value("bathroom_count", bathroom_count)
                else:
                    try:
                        bathroom_count = w2n.word_to_num(room_count)
                        item_loader.add_value("bathroom_count", bathroom_count)
                    except:
                        pass

        latlng = "".join(response.xpath("//script[@type='text/javascript']/text()").extract())
        if latlng:
            lat = latlng.split("CheckStreetViewAvailability(")[1].split('""')[0]
            item_loader.add_value("latitude", lat.split(",")[0])
            item_loader.add_value("longitude", lat.split(",")[1].replace(",",""))
                        
        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//li[contains(.,'Balcon') or contains(.,'balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//span[@class='bulleted-list']/div/span/ul/li[contains(.,'parking')]").get()
            if parking:
                item_loader.add_value("parking", True)

        balcony = response.xpath("//span[@class='bulleted-list']/div/span/ul/li[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = response.xpath("//li[contains(.,'lift') or contains(.,'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        desc = "".join(response.xpath("//div/h2[contains(.,'Desc')]/../div//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        images = [x for x in response.xpath("//div[@id='gallery-1']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        available_date = response.xpath("//span//li[contains(.,'Available') or contains(.,'available')]/text()").get()
        if available_date:
            available_date = available_date.lower().split("available")[1].strip()
            date = "{} {} ".format(available_date.replace("of",""), datetime.now().year)
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                current_date = str(datetime.now())
                if current_date > date2:
                    date = datetime.now().year + 1
                    parsed = date2.replace(str(date_parsed.year), str(date))
                    item_loader.add_value("available_date", parsed)
                item_loader.add_value("available_date", date2)

        
        item_loader.add_value("landlord_name", "DANIEL FORD")
        item_loader.add_value("landlord_phone", "020 7713 0909")
        item_loader.add_value("landlord_email", "city@danielforduk.com")
                

        yield item_loader.load_item()

def get_prop_type(p_text):
    if p_text and ("apartment" in p_text.lower() or "flat" in p_text.lower() or "maisonette" in p_text.lower()):
        p_result = "apartment"
    elif p_text and "house" in p_text.lower():
        p_result = "house"
    elif p_text and "studio" in p_text.lower():
        p_result = "studio"
    else:
        p_result = None

    return p_result

    
