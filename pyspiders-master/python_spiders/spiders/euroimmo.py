# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
import json
import html
from ..loaders import ListingLoader
from python_spiders.helper import string_found

class EuroimmoSpider(scrapy.Spider):
    name = "euroimmo"
    allowed_domains = ["www.euroimmo.be"]
    start_urls = (
        'http://www.www.euroimmo.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','
    
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36',
        'content-type': 'application/json',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
    }
    def start_requests(self):
        url = "https://www.euroimmo.be/Modules/ZoekModule/RESTService/SearchService.svc/GetPropertiesJSON/0/0"
        start_urls = [
            {
                'url': 'https://www.euroimmo.be/Modules/ZoekModule/RESTService/SearchService.svc/GetPropertiesJSON/0/0', 
                'property_type': 'apartment', 
                'data': {
                    "Transaction":"2",
                    "Type":"0",
                    "City":"0",
                    "MinPrice":"0",
                    "MaxPrice":"0",
                    "MinSurface":"0",
                    "MaxSurface":"0",
                    "MinSurfaceGround":"0",
                    "MaxSurfaceGround":"0",
                    "MinBedrooms":"0",
                    "MaxBedrooms":"0",
                    "Radius":"0",
                    "NumResults":"24",
                    "StartIndex":"1",
                    "ExtraSQL":"0",
                    "ExtraSQLFilters":"0",
                    "NavigationItem":"0",
                    "PageName":"0",
                    "Language":"NL",
                    "CountryInclude":"0",
                    "CountryExclude":"0",
                    "Token":"NVQRKGBDJNTISRRGABNFMCMPFJDNPHIUYDNIUAADPUHJZIGCZG",
                    "SortField":"1",
                    "OrderBy":"1",
                    "UsePriceClass":"true",
                    "PriceClass":"0",
                    "SliderItem":"0",
                    "SliderStep":"0",
                    "CompanyID":"0",
                    "SQLType":"3",
                    "MediaID":"0",
                    "PropertyName":"0",
                    "PropertyID":"0",
                    "ShowProjects":"false",
                    "Region":"0",
                    "currentPage":"0",
                    "homeSearch":"0",
                    "officeID":"0",
                    "menuIDUmbraco":"0",
                    "investment":"false",
                    "useCheckBoxes":"false",
                    "CheckedTypes":"0",
                    "newbuilding":"false",
                    "bedrooms":"0",
                    "latitude":"0",
                    "longitude":"0",
                    "ShowChildrenInsteadOfProject":"false",
                    "state":"0",
                    "FilterOutTypes":"16,27,20,37,26,8,7,5,11,39,38,1721,1722"
                }
            },
            {
                'url': 'https://www.euroimmo.be/Modules/ZoekModule/RESTService/SearchService.svc/GetPropertiesJSON/0/0', 
                'property_type': 'house',
                'data': {
                    "Transaction":"2",
                    "Type":"0",
                    "City":"0",
                    "MinPrice":"0",
                    "MaxPrice":"0",
                    "MinSurface":"0",
                    "MaxSurface":"0",
                    "MinSurfaceGround":"0",
                    "MaxSurfaceGround":"0",
                    "MinBedrooms":"0",
                    "MaxBedrooms":"0",
                    "Radius":"0",
                    "NumResults":"24",
                    "StartIndex":"1",
                    "ExtraSQL":"0",
                    "ExtraSQLFilters":"0",
                    "NavigationItem":"0",
                    "PageName":"0",
                    "Language":"NL",
                    "CountryInclude":"0",
                    "CountryExclude":"0",
                    "Token":"NVQRKGBDJNTISRRGABNFMCMPFJDNPHIUYDNIUAADPUHJZIGCZG",
                    "SortField":"1",
                    "OrderBy":"1",
                    "UsePriceClass":"true",
                    "PriceClass":"0",
                    "SliderItem":"0",
                    "SliderStep":"0",
                    "CompanyID":"0",
                    "SQLType":"3",
                    "MediaID":"0",
                    "PropertyName":"0",
                    "PropertyID":"0",
                    "ShowProjects":"false",
                    "Region":"0",
                    "currentPage":"0",
                    "homeSearch":"0",
                    "officeID":"0",
                    "menuIDUmbraco":"0",
                    "investment":"false",
                    "useCheckBoxes":"false",
                    "CheckedTypes":"0",
                    "newbuilding":"false",
                    "bedrooms":"0",
                    "latitude":"0",
                    "longitude":"0",
                    "ShowChildrenInsteadOfProject":"false",
                    "state":"0",
                    "FilterOutTypes":"28,29,35,30,36,12,31,32,33,34,16,27,20,37,11,39,38,1723,1722"
                }
            }
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                method='POST',
                body=json.dumps(url.get('data')),
                headers=self.headers,
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        try:
            datas = json.loads(response.text)
        except:
            datas = ''
        if datas:
            for data in datas:
                if 'Garage' in data['Property_HeadType_Value']:
                    continue 
                external_link = 'https://www.euroimmo.be' + data['Property_URL']
                external_id = data['FortissimmoID']
                title = data['Property_Title']
                description = html.unescape(data['Property_Description'])
                try:
                    square_meters = data['Property_Area_Build']
                except:
                    square_meters = ''
                try:
                    rent = str(data['Property_Price']) + "€"
                except:
                    rent = ''
                latitude = str(data['Property_Lat'])
                longitude = str(data['Property_Lon'])
                zipcode = data['Property_Zip']
                latitude = str(data['Property_Lat'])
                longitude = str(data['Property_Lon'])
                zipcode = str(data['Property_Zip'])
                room_count = str(data['bedrooms'])
                city = data['Property_City_Value']
                address = zipcode + ' ' + city + ' ' + data['Property_Street'] + ' ' + data['Property_Number']
                yield scrapy.Request(
                    url=external_link, 
                    callback=self.get_property_details, 
                    meta={
                        'property_type': response.meta.get('property_type'),
                        'external_id': external_id,
                        'title': title,
                        'description': description,
                        'square_meters': square_meters,
                        'rent': rent,
                        'latitude': latitude,
                        'longitude': longitude,
                        'zipcode': zipcode,
                        'room_count': room_count,
                        'city': city,
                        'address': address 
                    },
                    dont_filter=True
                )
    def get_property_details(self, response):
        property_type = response.meta.get('property_type')
        landlord_name = response.xpath('//div[@class="vertegenwoordiger"]/h3/text()').extract_first()
        landlord_phone = response.xpath('//a[@class="phone-vert"]/@href').extract_first()
        item_loader = ListingLoader(response=response)
        if property_type:
            item_loader.add_value('property_type', property_type)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("title", response.meta.get('title'))
        item_loader.add_value('address', response.meta.get('address'))
        item_loader.add_value('city', response.meta.get('city'))
        item_loader.add_value('zipcode', response.meta.get('zipcode'))
        item_loader.add_value('description', response.meta.get('description'))
        item_loader.add_value('rent_string', response.meta.get('rent'))
        item_loader.add_xpath('images', '//div[@id="galleryDetail"]//img[@class="rsTmb"]/@src')
        item_loader.add_value('square_meters', response.meta.get('square_meters'))
        item_loader.add_value('room_count', response.meta.get('room_count'))
        bathroom_count = response.xpath("//td[contains(.,'Badkamer')]//following-sibling::td//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        external_id = response.xpath("//div[@class='row']//h2[@class='redH3']//following-sibling::h3//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        furnished = response.xpath("//td[contains(.,'Bemeubeld')]//following-sibling::td//text()[contains(.,'Ja')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        item_loader.add_value("latitude", response.meta.get('latitude'))
        item_loader.add_value("longitude", response.meta.get('longitude'))
        
        item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_value('landlord_email', "info@euroimmo.be")
        if landlord_name:
            item_loader.add_value('landlord_phone', landlord_phone.replace('tel:', ''))
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()
