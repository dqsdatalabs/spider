# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin


class MySpider(Spider):
    name = 'akdamaremlak_com'
    execution_type='testing'
    country='turkey'
    locale='tr'
   
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://akdamaremlak.com.tr/tr/ilanlar?hdnAdTypeId=2&hdnProcessTypeId=87&hdnCityId=&hdnCountyId=null&hdnDistrictId=null&PageSizeFilter=&OrderText=&EnabledBankCredit=&MinPrice2=&MaxPrice2=&hdnSelectedHousePropertyTypeId=%5B133%2C127%2C120%2C125%2C121%2C134%2C141%2C142%2C665%5D&hdnSelectedHouseBalcoonCountId=null&hdnSelectedHouseBathroomCountId=null&hdnSelectedHouseBuildAgeId=null&hdnSelectedHouseBuildFloorTypeId=null&hdnSelectedHouseBuildTypeId=null&hdnSelectedHouseFloorTypeId=null&hdnSelectedHouseFuelTypeId=null&hdnSelectedHouseFullnessStatusId=null&hdnSelectedHouseHeatingTypeId=null&hdnSelectedHouseRealEstateCurrentStatusId=null&hdnSelectedHouseRegisterStatusId=null&hdnSelectedHouseRoomCountId=null&hdnSelectedHouseWcCountId=null&hdnSelectedHousePropertyShapeId=null&hdnSelectedCommercialBuildAgeId=null&hdnSelectedCommercialFullnessStatusId=null&hdnSelectedCommercialHeatingTypeId=null&hdnSelectedCommercialPropertyTypeId=null&hdnSelectedCommercialRegisterStatusId=null&hdnSelectedCommercialFloorTypeId=null&hdnSelectedCommercialBuildFloorTypeId=null&hdnSelectedLandFullnessStatusId=null&hdnSelectedLandPropertyTypeId=null&hdnSelectedLandRegisterStatusId=null&hdnSelectedTouristicResortHeatingTypeId=null&hdnSelectedTouristicResortPropertyTypeId=null&hdnSelectedTouristicResortRealEstateCurrentStatusId=null&hdnSelectedTouristicResortRegisterStatusId=null&hdnSelectedTouristicResortStarCountId=null&AdBaseId=&AdTypeId=2&ProcessTypeId=87&MinPrice=&MaxPrice=&CurrencyId=1&MinCleanArea=&MaxCleanArea=&CityId=&SelectedHousePropertyTypeId=119&SelectedHousePropertyTypeId=129&IsHousingState=&IsStudentAppropriate=&IsSingleAppropriate=&HouseExchangeForFlat=&CommercialExchangeForFlat=&LandExchangeForFlat=&TouristicResortIsHousingState=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://akdamaremlak.com.tr/tr/ilanlar?hdnAdTypeId=2&hdnProcessTypeId=87&hdnCityId=&hdnCountyId=null&hdnDistrictId=null&PageSizeFilter=&OrderText=&EnabledBankCredit=&MinPrice2=&MaxPrice2=&hdnSelectedHousePropertyTypeId=%5B119%2C129%5D&hdnSelectedHouseBalcoonCountId=null&hdnSelectedHouseBathroomCountId=null&hdnSelectedHouseBuildAgeId=null&hdnSelectedHouseBuildFloorTypeId=null&hdnSelectedHouseBuildTypeId=null&hdnSelectedHouseFloorTypeId=null&hdnSelectedHouseFuelTypeId=null&hdnSelectedHouseFullnessStatusId=null&hdnSelectedHouseHeatingTypeId=null&hdnSelectedHouseRealEstateCurrentStatusId=null&hdnSelectedHouseRegisterStatusId=null&hdnSelectedHouseRoomCountId=null&hdnSelectedHouseWcCountId=null&hdnSelectedHousePropertyShapeId=null&hdnSelectedCommercialBuildAgeId=null&hdnSelectedCommercialFullnessStatusId=null&hdnSelectedCommercialHeatingTypeId=null&hdnSelectedCommercialPropertyTypeId=null&hdnSelectedCommercialRegisterStatusId=null&hdnSelectedCommercialFloorTypeId=null&hdnSelectedCommercialBuildFloorTypeId=null&hdnSelectedLandFullnessStatusId=null&hdnSelectedLandPropertyTypeId=null&hdnSelectedLandRegisterStatusId=null&hdnSelectedTouristicResortHeatingTypeId=null&hdnSelectedTouristicResortPropertyTypeId=null&hdnSelectedTouristicResortRealEstateCurrentStatusId=null&hdnSelectedTouristicResortRegisterStatusId=null&hdnSelectedTouristicResortStarCountId=null&AdBaseId=&AdTypeId=2&ProcessTypeId=87&MinPrice=&MaxPrice=&CurrencyId=1&MinCleanArea=&MaxCleanArea=&CityId=&SelectedHousePropertyTypeId=133&SelectedHousePropertyTypeId=127&SelectedHousePropertyTypeId=120&SelectedHousePropertyTypeId=126&SelectedHousePropertyTypeId=125&SelectedHousePropertyTypeId=121&SelectedHousePropertyTypeId=134&SelectedHousePropertyTypeId=141&SelectedHousePropertyTypeId=142&SelectedHousePropertyTypeId=665&IsHousingState=&IsStudentAppropriate=&IsSingleAppropriate=&HouseExchangeForFlat=&CommercialExchangeForFlat=&LandExchangeForFlat=&TouristicResortIsHousingState=",
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):        
        for item in response.xpath("//div[@class='info']/h3/a/@href").getall():
            follow_url = response.urljoin(item)        
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta["property_type"]})
                        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "akdamaremlak_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_id", response.xpath("//div[@id='property-id']/text()").extract_first().split(":")[-1])
        title = response.xpath("//h1/span/text()").extract_first()
        item_loader.add_value("title", title)

        rent = response.xpath("//dt[.='Fiyat:']/following-sibling::dd[1]//text()").extract_first()
        if rent:
            item_loader.add_value("rent", rent.split(",")[0])
            item_loader.add_value("currency", "TRY")
        floor = response.xpath("//dt[.='Bulunduğu Kat:']/following-sibling::dd[1]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        square_meters = response.xpath("//dt[.='Brüt Alan']/following-sibling::dd[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())
        address = response.xpath("//h1/small/text()").extract_first()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[0])

        room_count =response.xpath("//dt[.='Oda:']/following-sibling::dd[1]//text()").extract_first()
        if room_count and "+" in  room_count:
            item_loader.add_value("room_count", split_room(room_count))
        else:
            item_loader.add_value("room_count", room_count)
        item_loader.add_xpath("bathroom_count", "//dt[.='Banyo Sayısı:']/following-sibling::dd[1]/text()")
        item_loader.add_xpath("deposit", "//dt[contains(.,'Depozito')]/following-sibling::dd[1]/text()[.!='-']")
        item_loader.add_xpath("utilities", "//dt[contains(.,'Aidat')]/following-sibling::dd[1]/text()[.!='-']")

        desc = "".join(response.xpath("//h1[@class='section-title' and .='Detaylı Açıklama']/following-sibling::p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())  
            
                
        balcony=response.xpath("//dt[contains(.,'Balkon Sayısı')]/following-sibling::dd[1]/text()[.!='0']").extract_first()
        if balcony:
            item_loader.add_value("balcony",True)
                
        images = [x for x in response.xpath("//div[@u='slides']//img[@u='image']/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath("landlord_name","//div[@class='info']//h2/a/text()")
        item_loader.add_xpath("landlord_phone","//ul[@class='contact-us']/li[i[@class='fa fa-phone']]/a/text()")

        email=response.xpath("//ul[@class='contact-us']/li[i[@class='fa fa-envelope']]/a/text()").get()
        if email:
            item_loader.add_value("landlord_email", email.strip())
 
        yield item_loader.load_item()
def split_room(room_count):
    add=0
    room_array=room_count.split("+")
    for i in room_array:
        add += int(i)
    return str(add)