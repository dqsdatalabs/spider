# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import copy
import requests
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import NotSupported
from .helper import string_found, extract_rent_currency, convert_string_to_numeric


def filter_null_objects(_item):
    _new_item = copy.deepcopy(_item)
    for key in _item:
        if not _item[key]:
            del _new_item[key]
    return _new_item


def remove_invalid_data(_json):
    _new_json = []
    for _item in _json:
        if not _item.get('external_link', None) \
                or not _item.get('address', None) \
                or not _item.get('property_type', None) \
                or not _item.get('square_meters', None) \
                or not _item.get('room_count', None) \
                or not _item.get('rent', None) \
                or not _item.get('currency', None):
                    pass
        else:
            _new_json.append(_item)
    return _new_json


def call_reva_api(_json, _country, _locale, _extraction_type):
    settings = get_project_settings()
    headers = {'X-Report-Formats': 'summary+attributewise_error_summary+error_details+missing_values',
               'X-Country': _country,
               'X-Locale': _locale}
    if _extraction_type == 'production':
        # Logic to remove invalidate properties for production entry
        base_url = "{}/api/spiders/process_data".format(
            settings.get('API_ENDPOINT'))
        response = requests.post(base_url, headers=headers, json=_json)
        print(response.content.decode('utf-8'))
    else:
        base_url = "{}/api/spiders/validate".format(
            settings.get('API_ENDPOINT'))
        response = requests.post(base_url, headers=headers, json=_json)
        print(response.content.decode('utf-8'))
    return response

def call_reva_api_no_item(_json, _country, _locale, _extraction_type, _external_source):
    settings = get_project_settings()
    headers = {'X-Report-Formats': 'summary+attributewise_error_summary+error_details+missing_values',
               'X-Country': _country,
               'X-Locale': _locale,
               'X-Spider':_external_source
               
               }
    if _extraction_type == 'production':
        # Logic to remove invalidate properties for production entry
        base_url = "{}/api/spiders/process_data".format(
            settings.get('API_ENDPOINT'))
        response = requests.post(base_url, headers=headers, json=_json)
        print(response.content.decode('utf-8'))
    else:
        base_url = "{}/api/spiders/validate".format(
            settings.get('API_ENDPOINT'))
        response = requests.post(base_url, headers=headers, json=_json)
        print(response.content.decode('utf-8'))
    return response

class PythonSpidersPipeline:
    all_items = []

    def open_spider(self, spider):
        self.all_items = []

    def close_spider(self, spider):
        batch = 100000
        print("Inside Closing spider")
        # api_response = call_reva_api(self.all_items, spider.country, spider.locale, spider.execution_type)
        if self.all_items:
            for idx in range(0, len(self.all_items), batch):
                api_response = call_reva_api(
                    self.all_items[idx:idx + batch], spider.country, spider.locale, spider.execution_type)
                if api_response.status_code == 200:
                    spider.log('Response from validation API {}'.format(
                        api_response.content.decode('utf-8')))
                else:
                    raise NotSupported("Error while calling API Response-code: {}, response-body: {}".format(
                        api_response.status_code, api_response.content.decode('utf-8')))
        else:
            spider.log('No items')

            try:
        
                api_response = call_reva_api_no_item(
                    self.all_items, spider.country, spider.locale, spider.execution_type, spider.external_source)
                if api_response.status_code == 200:
                    spider.log('Response from validation API {}'.format(
                        api_response.content.decode('utf-8')))
                else:
                    raise NotSupported("Error while calling API Response-code: {}, response-body: {}".format(
                        api_response.status_code, api_response.content.decode('utf-8')))
            except Exception as error:
                print('Caught this error: ' + repr(error))
  

    def process_item(self, item, spider):
        images = item.get('images', None)
        floor_plan_images = item.get('floor_plan_images', None)
        description = item.get('description', None)
        rent_string = item.get('rent_string', None)
        rent = item.get('rent', None)
        square_meters = item.get('square_meters', None)
        utilities = item.get('utilities', None)
        deposit = item.get('deposit', None)
        heating_cost = item.get('heating_cost', None)
        water_cost = item.get('water_cost', None)
        prepaid_rent = item.get('prepaid_rent', None)
        if images:
            item["images"] = list(set(item["images"]))
            item['external_images_count'] = len(item['images'])
        if floor_plan_images:
            item["floor_plan_images"] = list(set(item["floor_plan_images"]))


        # if description:
        #     if string_found(['parking', 'parkeerplaats','garage','otopark'], description):
        #         item['parking'] = True
        #     if string_found(['balcon','balkon','Balcony'], description):
        #         item['balcony'] = True
        #     if string_found(['sans ascenseur'], description):
        #         item['elevator'] = False
        #     elif string_found(['ascenseur','lift','elevator'], description):
        #         item['elevator'] = True
        #     if string_found(['terrasse', 'terrace',"terras"], description):
        #         item['terrace'] = True
        #     if string_found(['dishwasher','vaatwas','vaatwasser','lave-vaisselle','lavavajillas','lave vaisselle'], description):
        #         item['dishwasher'] = True
        #     if string_found(['wasmachine',"lave linge",'lavadora',"lave-linge"], description):
        #         item['washing_machine'] = True
        #     if string_found(['unfurnished'], description):
        #         item['furnished'] = False
        #     elif string_found(["meuble","meublé"], description):
        #         item['furnished'] = True
        external_source = item.get('external_source', None)
        if rent_string and external_source:
            rent, currency = extract_rent_currency(rent_string, external_source, spider)
            item['rent'] = convert_string_to_numeric(rent, spider)
            if not item.get('currency', None):
                item['currency'] = currency
            del item['rent_string']
        if rent:
            item['rent'] = convert_string_to_numeric(rent, spider)
        if square_meters:
            item['square_meters'] = convert_string_to_numeric(square_meters, spider)
        if utilities:
            item['utilities'] = convert_string_to_numeric(utilities, spider)
        if deposit:
            item['deposit'] = convert_string_to_numeric(deposit, spider)
        if heating_cost:
            item['heating_cost'] = convert_string_to_numeric(heating_cost, spider)
        if water_cost:
            item['water_cost'] = convert_string_to_numeric(water_cost, spider)
        if prepaid_rent:
            item['prepaid_rent'] = convert_string_to_numeric(prepaid_rent, spider)
        new_item = filter_null_objects(item)
        self.all_items.append(dict(new_item))
        return item