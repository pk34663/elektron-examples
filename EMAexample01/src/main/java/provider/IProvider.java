package provider;

import java.util.Vector;

import com.thomsonreuters.ema.access.EmaFactory;
import com.thomsonreuters.ema.access.FieldList;
import com.thomsonreuters.ema.access.GenericMsg;
import com.thomsonreuters.ema.access.Msg;
import com.thomsonreuters.ema.access.OmmException;
import com.thomsonreuters.ema.access.OmmIProviderConfig;
import com.thomsonreuters.ema.access.OmmProvider;
import com.thomsonreuters.ema.access.OmmProviderClient;
import com.thomsonreuters.ema.access.OmmProviderEvent;
import com.thomsonreuters.ema.access.OmmReal;
import com.thomsonreuters.ema.access.OmmState;
import com.thomsonreuters.ema.access.PostMsg;
import com.thomsonreuters.ema.access.RefreshMsg;
import com.thomsonreuters.ema.access.ReqMsg;
import com.thomsonreuters.ema.access.StatusMsg;
import com.thomsonreuters.ema.rdm.EmaRdm;

class AppClient implements OmmProviderClient
{
    public Vector<Long> itemList = new Vector<Long>();
    public boolean login = false;

    public void onReqMsg(ReqMsg reqMsg, OmmProviderEvent event)
    {
        switch (reqMsg.domainType())
        {
            case EmaRdm.MMT_LOGIN :
                processLoginRequest(reqMsg, event);
                break;
            case EmaRdm.MMT_MARKET_PRICE :
                processMarketPriceRequest(reqMsg, event);
                break;
            default :
                processInvalidItemRequest(reqMsg, event);
                break;
        }
    }

    public void onRefreshMsg(RefreshMsg refreshMsg, OmmProviderEvent event){}
    public void onStatusMsg(StatusMsg statusMsg, OmmProviderEvent event){}
    public void onGenericMsg(GenericMsg genericMsg, OmmProviderEvent event){}
    public void onPostMsg(PostMsg postMsg, OmmProviderEvent event){}
    public void onReissue(ReqMsg reqMsg, OmmProviderEvent event){}

    public void onClose(ReqMsg reqMsg, OmmProviderEvent event)
    {
        System.out.println("Connection closing");
        login = false;
        itemList.clear();
    }
    public void onAllMsg(Msg msg, OmmProviderEvent event){}

    void processLoginRequest(ReqMsg reqMsg, OmmProviderEvent event)
    {
        System.out.println("Processing Login Request:");
        event.provider().submit( EmaFactory.createRefreshMsg().domainType(EmaRdm.MMT_LOGIN).name(reqMsg.name()).
                        nameType(EmaRdm.USER_NAME).complete(true).solicited(true).
                        state(OmmState.StreamState.OPEN, OmmState.DataState.OK, OmmState.StatusCode.NONE, "Login accepted"),
                event.handle() );

        login = true;
    }

    void processMarketPriceRequest(ReqMsg reqMsg, OmmProviderEvent event)
    {
        System.out.println("Processing Market Price Request");
        /*
        if( itemHandle != 0 )
        {
            processInvalidItemRequest(reqMsg, event);
            return;
        }*/

        System.out.printf("Name: %s Service: %s\n",reqMsg.name(),reqMsg.serviceName());
        FieldList fieldList = EmaFactory.createFieldList();
        fieldList.add( EmaFactory.createFieldEntry().real(22, 3990, OmmReal.MagnitudeType.EXPONENT_NEG_2));
        fieldList.add( EmaFactory.createFieldEntry().real(25, 3994, OmmReal.MagnitudeType.EXPONENT_NEG_2));
        fieldList.add( EmaFactory.createFieldEntry().real(30, 9,  OmmReal.MagnitudeType.EXPONENT_0));
        fieldList.add( EmaFactory.createFieldEntry().real(31, 19, OmmReal.MagnitudeType.EXPONENT_0));

        try {
            event.provider()
                    .submit(EmaFactory.createRefreshMsg()
                            .name(reqMsg.name())
                            .serviceId(reqMsg.serviceId())
                            .solicited(true)
                            .state(OmmState.StreamState.OPEN, OmmState.DataState.OK, OmmState.StatusCode.NONE, "Refresh Completed")
                            .payload(fieldList).complete(true), event.handle());

            itemList.add(event.handle());
        } catch (Exception e)
        {
            System.out.println("update message threw");
        }
    }

    void processInvalidItemRequest(ReqMsg reqMsg, OmmProviderEvent event)
    {
        System.out.println("Processing Invalid Item Request");
        event.provider().submit( EmaFactory.createStatusMsg().name(reqMsg.name()).serviceName(reqMsg.serviceName()).
                        state(OmmState.StreamState.CLOSED, OmmState.DataState.SUSPECT,  OmmState.StatusCode.NOT_FOUND, "Item not found"),
                event.handle() );
    }
}

public class IProvider
{
    static OmmProvider provider = null;
    static AppClient appClient;

    public static void publishUpdates() {
        FieldList fieldList = EmaFactory.createFieldList();
        //Wait for item requests before publishing on them
        try {
            while (appClient.itemList.size() == 0)
                Thread.sleep(1000);
        } catch (Exception e)
        {}

        try {
            for (int i = 0; i < 60 && appClient.login == true; i++) {
                for (Long v : appClient.itemList) {
                    System.out.println("Sending update");
                    fieldList.clear();
                    fieldList.add(EmaFactory.createFieldEntry().real(22, 3991 + i, OmmReal.MagnitudeType.EXPONENT_NEG_2));
                    fieldList.add(EmaFactory.createFieldEntry().real(30, 10 + i, OmmReal.MagnitudeType.EXPONENT_0));

                    provider.submit(EmaFactory.createUpdateMsg().payload(fieldList), v);
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            System.out.println("throwing while sending update");
        }
    }

    public static void main(String[] args)
    {
        try
        {
            appClient = new AppClient();
            FieldList fieldList = EmaFactory.createFieldList();

            OmmIProviderConfig config = EmaFactory.createOmmIProviderConfig();

            provider = EmaFactory.createOmmProvider(config.port("14002"), appClient);

            while(true)
                publishUpdates();
        }
        catch (OmmException excp)
        {
            System.out.println("Caught exception in main");
            System.out.println(excp.getMessage());
        }
        finally
        {
            System.out.println("Cleaning up");
            if (provider != null) provider.uninitialize();
        }
    }
}