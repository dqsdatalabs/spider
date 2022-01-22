# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import dateparser

class MySpider(Spider):
    name = 'kwturkiye_com_kwbornova'
    execution_type='testing'
    country='turkey'
    locale='tr'

    headers = {
        'accept': "application/json, text/plain, */*",
        'accept-encoding': "gzip, deflate, br",
        'accept-language': "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        'content-type': "application/json;charset=UTF-8",
        'origin': "https://www.kwturkiye.com",
        'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
    }
    custom_settings = {
        "PROXY_ON": True,
        #"PROXY_PR_ON": True,
        "PASSWORD": "wmkpu9fkfzyo",
    }

    def start_requests(self):
        start_urls = [
            {
                "property_code" : "[1012]",
                "property_type" : "apartment",
                "office_id" : "1010",
                "office_name" :"KW Karşıyaka",
            },
            {
                "property_code" : "[1014, 1013]",
                "property_type" : "house",
                "office_id" : "1010",
                "office_name" :"KW Karşıyaka",
            },
            {
                "property_code" : "[1012]",
                "property_type" : "apartment",
                "office_id" : "1006",
                "office_name" :"KW Bornova",
            },
            {
                "property_code" : "[1014, 1013]",
                "property_type" : "house",
                "office_id" : "1006",
                "office_name" :"KW Bornova",
            },
        ]
        for url in start_urls:
            payload = "{\"skipCount\":0,\"maxResultCount\":24,\"transactionType\":\"2\",\"listingClass\":\"1\",\"macroProvinceId\":null,\"provinceId\":null,\"cityId\":null,\"localZoneIds\":[],\"currency\":\"TRY\",\"maxPrice\":null,\"minPrice\":null,\"propertyType\":" + url.get("property_code") + ",\"bedRoomCount\":null,\"bathRoomCount\":null,\"squareMeter\":null,\"parkingSpace\":null,\"totalRoomCount\":null,\"totalAreaSqMeter\":null,\"lotSizeSqMeter\":null,\"developmentId\":null,\"buildingId\":null,\"energyLevel\":null,\"addedDay\":0,\"agentId\":0,\"officeId\":\"" + url.get("office_id") + "\",\"teamId\":0,\"officeName\":\"" + url.get("office_name") + "\",\"agentName\":\"\",\"upComingOpenHouse\":null,\"orderByCriteria\":1026,\"id\":null,\"tenantId\":1,\"regionId\":2004,\"districtIds\":[],\"measurementType\":1,\"freeTextSearch\":\"\",\"homeRegionCurrencyUID\":146,\"homeRegionId\":2004,\"isLuxury\":false,\"station\":null,\"isVirtualOpenHouse\":false,\"taxIncluded\":false}"
            
            yield Request(
                "https://www.kwturkiye.com/api/services/app/listings/GetListingsForWebSite",
                method="POST",
                body=payload,
                headers=self.headers,
                callback=self.parse,
                meta={
                    'property_type': url.get('property_type'),
                    "property_code": url.get("property_code"),
                    "office_id" : url.get("office_id"),
                    "office_name" : url.get("office_name"),
                }
            )


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 24)
        
        seen = False
        data = json.loads(response.body)
        for item in data["result"]["items"]:
            ext_url = "https://www.kwturkiye.com/" + item["seoFriendlyURL_Local"]
            filter_id = item["listingId"]
            payload = "{\"filter\":\"" + filter_id + "\",\"bpId\":\"\",\"hashedAgentId\":\"\",\"defaultCurrencyISO\":\"tr-TR\",\"regionId\":2004,\"tenantId\":1,\"homeRegionCurrencyUID\":146}"
            yield Request(
                "https://www.kwturkiye.com/api/services/app/listings/GetListingForWebSite",
                method="POST",
                body=payload,
                headers=self.headers,
                callback=self.populate_item,
                meta={
                    'property_type': response.meta.get('property_type'),
                    "ext_url" : ext_url,
                }
            )
            seen = True


        if page == 24 or seen:
            payload = "{\"skipCount\":" + str(page) + ",\"maxResultCount\":24,\"transactionType\":\"2\",\"listingClass\":\"1\",\"macroProvinceId\":null,\"provinceId\":null,\"cityId\":null,\"localZoneIds\":[],\"currency\":\"TRY\",\"maxPrice\":null,\"minPrice\":null,\"propertyType\":" + response.meta.get("property_code") + ",\"bedRoomCount\":null,\"bathRoomCount\":null,\"squareMeter\":null,\"parkingSpace\":null,\"totalRoomCount\":null,\"totalAreaSqMeter\":null,\"lotSizeSqMeter\":null,\"developmentId\":null,\"buildingId\":null,\"energyLevel\":null,\"addedDay\":0,\"agentId\":0,\"officeId\":\"" + response.meta.get("office_id") + "\",\"teamId\":0,\"officeName\":\"" + response.meta.get("office_name") + "\",\"agentName\":\"\",\"upComingOpenHouse\":null,\"orderByCriteria\":1026,\"id\":null,\"tenantId\":1,\"regionId\":2004,\"districtIds\":[],\"measurementType\":1,\"freeTextSearch\":\"\",\"homeRegionCurrencyUID\":146,\"homeRegionId\":2004,\"isLuxury\":false,\"station\":null,\"isVirtualOpenHouse\":false,\"taxIncluded\":false}"
            yield Request(
                "https://www.kwturkiye.com/api/services/app/listings/GetListingsForWebSite",
                method="POST",
                body=payload,
                headers=self.headers,
                callback=self.parse,
                meta={
                    'property_type': response.meta.get('property_type'),
                    "property_code": response.meta.get("property_code"),
                    "page": page+24,
                    "office_id" : response.meta.get("office_id"),
                    "office_name" : response.meta.get("office_name"),
                }
            )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response) 

        data = json.loads(response.body)
        prop_type = response.meta.get("property_type")
        item_loader.add_value("title", data['result']['listing']['listingHeader'])
        item_loader.add_value("property_type", prop_type)

        item_loader.add_value("external_source", "Kwturkiyebornova_PySpider_"+ self.country + "_" + self.locale)

        address = data['result']['listing']['listingAddress']
        if address:
            item_loader.add_value("address", address.strip())
        
        city = data['result']['listing']['city']
        if city:
            item_loader.add_value("city", city.strip().split(' ')[-1])

        latitude = data['result']['listing']['latitude']
        longitude = data['result']['listing']['longitude']
        if latitude and longitude:
            latitude = str(latitude)
            longitude = str(longitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        square_meters = data['result']['listing']['livingAreaSqM']
        if square_meters:
            item_loader.add_value("square_meters", str(int(square_meters)))
                
        saloon_count = data['result']['listing']['totalNumberOfLivingRooms']
        room_count = data['result']['listing']['bedRooms']
        if room_count and saloon_count:
            room_count = room_count + saloon_count
            item_loader.add_value("room_count", str(room_count))
        else:
            room_count = response.xpath("//div[contains(.,'Oda')]/following-sibling::div/text()").re_first(r"\d+")
            if room_count:
                item_loader.add_value("room_count", room_count)
            
        bathroom_count = data['result']['listing']['bathRooms']
        print(bathroom_count)
        if bathroom_count:
            item_loader.add_value("bathroom_count", str(bathroom_count))
            
        rent = data['result']['listing']['listingPrice']
        if rent:
            item_loader.add_value("rent", str(rent).split(".")[0])

        currency = data['result']['listing']['listingCurrencyISO']
        item_loader.add_value("currency", currency)

        ext_url = response.meta.get("ext_url")
        item_loader.add_value("external_link", ext_url) 

        external_id = data['result']['listing']['listingKey']
        if external_id:
            item_loader.add_value("external_id", external_id)

        description = data['result']['listing']['listingDescription'][1]['description']  
        if description:
            filt = HTMLFilter()
            filt.feed(description)
            item_loader.add_value("description", filt.text)

        city = data['result']['listing']['macroProvince']
        if city:
            item_loader.add_value("city", city)

        available_date = data['result']['listing']['dateAvailable']
        if not available_date.startswith('0001-01-01'):
            if available_date and available_date.isalpha() != True:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        images = [x['originalImageUrl'] for x in data['result']['listing']['listingImages']['items']]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        floor_plan_images = data['result']['listing']['floorPlanUrl']
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        features = data['result']['listing']['listingFeatures']
        for category in features:
            for subfeatures in category['listingFeatures']:
                if subfeatures['listingFeatureValue'] == 'Mobilyasız':
                    item_loader.add_value("furnished", False)
                if subfeatures['listingFeatureValue'] == 'Mobilyalı':
                    item_loader.add_value("furnished", True)

        
        floor = data['result']['listing']['floor']
        if floor:
            item_loader.add_value("floor", floor)

        features = data['result']['listing']['listingFeatures']
        parking = None
        for category in features:
            for subfeatures in category['listingFeatures']:
                if subfeatures['listingFeatureValue'] == 'Otopark':
                    parking = bool(subfeatures['isSelected'])
        if parking:
            item_loader.add_value("parking", parking)

        features = data['result']['listing']['listingFeatures']
        elevator = None
        for category in features:
            for subfeatures in category['listingFeatures']:
                if subfeatures['listingFeatureValue'] == 'Asansör':
                    elevator = bool(subfeatures['isSelected'])
        if elevator:
            item_loader.add_value("elevator", elevator)

        features = data['result']['listing']['listingFeatures']
        balcony = None
        for category in features:
            for subfeatures in category['listingFeatures']:
                if subfeatures['listingFeatureValue'] == 'Balkon':
                    balcony = bool(subfeatures['isSelected'])
        if balcony:
            item_loader.add_value("balcony", balcony)

        features = data['result']['listing']['listingFeatures']
        terrace = None
        for category in features:
            for subfeatures in category['listingFeatures']:
                if subfeatures['listingFeatureValue'] == 'Teras':
                    terrace = bool(subfeatures['isSelected'])
        if terrace:
            item_loader.add_value("terrace", terrace)

        features = data['result']['listing']['listingFeatures']
        swimming_pool = None 
        for category in features:
            for subfeatures in category['listingFeatures']:
                if subfeatures['listingFeatureValue'] == 'Yüzme Havuzu':
                    swimming_pool = bool(subfeatures['isSelected'])
        if swimming_pool:
            item_loader.add_value("swimming_pool", swimming_pool)

        features = data['result']['listing']['listingFeatures']
        washing_machine = None
        dishwasher = None
        for category in features:
            for subfeatures in category['listingFeatures']:
                if subfeatures['listingFeatureValue'] == 'Beyaz Eşyalı':
                    washing_machine = bool(subfeatures['isSelected'])
                    dishwasher = bool(subfeatures['isSelected'])
        if washing_machine:
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("dishwasher", dishwasher)

        landlord_name = data['result']['listing']['agentFullName']
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = data['result']['listing']['agentMobilePhone']
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        landlordphonecheck=item_loader.get_output_value("landlord_phone")
        if not landlordphonecheck:
            item_loader.add_value("landlord_phone","+90212  5987")

        landlord_email = data['result']['listing']['agentEmail']
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data